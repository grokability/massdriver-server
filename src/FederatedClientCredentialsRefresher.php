<?php

namespace Massdriver;

use Amp\ByteStream\ReadableResourceStream;
use Amp\ByteStream\WritableResourceStream;
use Amp\DeferredFuture;
use Amp\Future;
use Dotenv\Dotenv;
use Massdriver\AmpAws\AmpAws;
use Revolt\EventLoop;
use function Amp\async;
use function Amp\ByteStream\buffer;
use function Amp\File\isFile;
use function Amp\File\listFiles;
use function Amp\Future\await;
use function Amp\Future\awaitAll;

function readFileStream(string $path): ReadableResourceStream
{
    if(!isFile($path)){
        touch($path); // this won't happen in 'real life' - but for testing it's useful
    }
    $resource = fopen($path, 'rb');

    if ($resource === false) {
        throw new \RuntimeException("Unable to open {$path}");
    }

    return new ReadableResourceStream($resource);
}

function writeFileStream(string $path,?string $same_as_file = null): WritableResourceStream
{
    $resource = fopen($path, 'w');

    if ($resource === false) {
        throw new \RuntimeException("Unable to open {$path}");
    }

    if($same_as_file) {
        $stat_results = stat($same_as_file);
        chown($path, $stat_results['uid']) ?: throw new \Exception("Couldn't change owner");
        chgrp($path, $stat_results['gid']) ?: throw new \Exception("Couldn't change group");
        chmod($path, $stat_results['mode']) ?: throw new \Exception("Couldn't change mode");
    }

    return new WritableResourceStream($resource);
}

class FederatedClientCredentialsRefresher implements GracefulShutdown
{
    protected static array $credentials_array = [];
    protected static array $refresh_credentials_timers = [];
    private string $signal;
    private ?Future $reload = null;
    protected AmpAws $sts_client;

    const array CREDENTIAL_MAP = [
        'AWS_ACCESS_KEY_ID' => 'AccessKeyId',
        'AWS_SECRET_ACCESS_KEY' => 'SecretAccessKey',
        'AWS_SESSION_TOKEN' => 'SessionToken',
        '_SESSION_EXPIRATION' => 'Expiration',
    ];
    const int MAX_EXPIRATION_THRESHOLD = 3600; //once you have an hour remaining, it's *time*!

    public function __construct(
        public string $directory,
        public string $inline_role = '',
        public array $arns = [],
    )
    {
        print "Constructiong Refresher Object\n";
        $this->sts_client = new AmpAws(
            'Sts',
            [
                'version' => '2011-06-15',
            ]
        );

        $this->signal = EventLoop::onSignal(SIGHUP, fn () => $this->load_credentials_from_disk() ? null: false);
        EventLoop::unreference($this->signal);

        $this->load_credentials_from_disk();
    }

    function graceful_shutdown(): void
    {
        print "ClientCredentials refresher is gracefully shutting down\n";
        foreach(static::$refresh_credentials_timers as $slug => $timer) {
            print "Timer for: $slug -> $timer\n";
            EventLoop::cancel($timer);
        }
    }

    public function env_path_for_tenant($tenant): string
    {
        return rtrim($this->directory, '/') . "/$tenant/.env";
    }

    public function load_credentials_from_disk(): Future
    {
        print "Loading credentials from disk\n";
        if ($this->reload !== null && !$this->reload->isComplete()) {
            return $this->reload;
        }
        $df = new DeferredFuture();
        $this->reload = $df->getFuture();
        $futures = [];
        foreach (listFiles($this->directory) as $tenant) {
            print "looking at tenant: $tenant\n";
            $filename = $this->env_path_for_tenant($tenant);
            if (!isFile($filename)) {
                // continue; In any kind of 'prod' environment, this wouldn't happen. But for testing it's at least useful
            }
            $futures[] = async(function () use ($filename, &$soonest, $tenant) { // parallelize each tenant's 'load'
                $env_file_stream = readFileStream($filename);
                $env_file_contents = buffer($env_file_stream);
                $credentials = array_intersect_key(Dotenv::parse($env_file_contents), self::CREDENTIAL_MAP);
                static::$credentials_array[$tenant] = $credentials; //could be 'empty array' for a new customer?

                // now, either REFRESH this token if it's already overdue, or *schedule* this one if it isn't?
                $this->single_credential_refresh_loop($tenant);
            });
        }

        $this->reload = async(function () use (&$futures, &$df) {
            try {
                $df->complete(awaitAll($futures));
            } catch (\Throwable $e) {
                print "Found an error while refreshing... something: ".$e->getMessage()."\n";
            } finally {
                $this->reload = null;
            }
        });

        return $this->reload;
    }

    public function single_credential_refresh_loop(string $tenant)
    {
        static $retry_count = 0;
        $credentials = static::$credentials_array[$tenant];

        $time_remaining_in_token = ($credentials['_SESSION_EXPIRATION'] ?? 0) - microtime(true);
        if ( $time_remaining_in_token < self::MAX_EXPIRATION_THRESHOLD) {
            if(self::$refresh_credentials_timers[$tenant] ?? false) {
                //determine if that timer is valid, and if so, cancel it?

                self::$refresh_credentials_timers[$tenant] = null;
            }
            try {
                $credential_future = $this->refresh_one_credential($tenant);
                $env_file_stream = readFileStream($this->env_path_for_tenant($tenant));
                $env_file_contents = async(fn () => buffer($env_file_stream));
                $this->write_one_credential($tenant, $credential_future, $env_file_contents)->await(); //make sure it completed before we write it into memory
                $credentials = $credential_future->await();
                self::$credentials_array[$tenant] = $credentials;

            } catch (\Throwable $exception) {
                //I mean, I guess we just try again? In a few minutes?
                $retry_delays = [5,10,30,45,60];
                $retry_count++;
                print "Exception caught when renewing credentials: $tenant: " . $exception->getMessage() . "\n";
                if($retry_count > 5) {
                    print "We've retried 5 times and still keep failing, bailing out!\n";
                    throw $exception;
                }
                //janky, but it's the best way I could make it work
                $credentials['_SESSION_EXPIRATION'] = time() + $retry_delays[$retry_count-1];
            }
        } else {
            //just make sure we already have a timer set. If not, then we got problems.
            if(self::$refresh_credentials_timers[$tenant] ?? false) {
                return;
            }
        }
        //now, set up a *new* timer
        $new_time_remaining_in_token = ($credentials['_SESSION_EXPIRATION'] ?? 0) - microtime(true);
        $delay_with_floor = max(0.1,$new_time_remaining_in_token - self::MAX_EXPIRATION_THRESHOLD);
        $timer_id = EventLoop::delay($delay_with_floor, fn () => $this->single_credential_refresh_loop($tenant));
        EventLoop::unreference($timer_id); //don't let timers keep the event loop up
        self::$refresh_credentials_timers[$tenant] = $timer_id;

}

    public function refresh_one_credential(string $tenant): Future
    {
        return async( function () use ($tenant) {
            $params = [
                'DurationSeconds' => 129_600, //maximum default - might make configurable
                'Name' => $tenant,
                // Policy will get subbed in here for $this->inline_policy (if set)
                // PolicyArns will get subbed in here for $this->$this->arns (also only if set)
                'Tags' => [
                    [
                        'Key' => 'slug',
                        'Value' => $tenant,
                    ]
                ]
            ];
            if($this->inline_role) {
                $params['Policy'] = $this->inline_role;
            }
            if($this->arns) {
                $policy_arns = [];
                foreach($this->arns as $arn) {
                    $policy_arns[] = ['arn' => $arn];
                }
                $params['PolicyArns'] = $policy_arns;
            }
            $result = $this->sts_client->getFederationTokenAsync($params);
            $credentials = [];
            $results = $result['Credentials'];
            foreach(self::CREDENTIAL_MAP as $env_name => $aws_name) {
                if(array_key_exists($aws_name, $results)) {
                    $credentials[$env_name] = $results[$aws_name];
                }
            }
            $credentials['_SESSION_EXPIRATION'] = strtotime($credentials['_SESSION_EXPIRATION']) ?: 0;
            return $credentials;
        });
    }

    public function write_one_credential(string $tenant, Future $credentials, Future $old_env_file):Future
    {
        $future = new DeferredFuture();
        try {
            $env_file = $this->env_path_for_tenant($tenant);
            $tmp_file = $env_file . ".tmp";
            $env_stream = writeFileStream($tmp_file,$env_file);

            //this is going to be, like, 4 async writes in a row - key, secret, session, _session_duration

            [$env_file_contents, $credentials_contents] = await([$old_env_file, $credentials]);
            $env_file_lines = explode("\n", $env_file_contents);
            foreach ($env_file_lines as $env_file_line) {
                $pieces = explode("=", $env_file_line, 2);
                $is_multiline = false;
                try {
                    // just trying to parse the individual *line* to see if it validates on its own
                    // if not, it must be multiline
                    Dotenv::parse($env_file_line);
                } catch (\Exception $e) {
                    $is_multiline = true;
                }
                if (count($pieces) == 2 && !$is_multiline) {
                    // env-assignment mode
                    [$name, $value] = $pieces;
                    if (array_key_exists($name, $credentials_contents)) {
                        $value = '"' . $credentials_contents[$name] . '"';
                        unset($credentials_contents[$name]); //delete is so we can check at the end if we missed anything
                    }
                    $env_stream->write("$name=$value\n");
                } else {
                    // 'literal' mode
                    // (works for comment-lines, blank-lines, and continuations of multi-line variables)
                    // Luckily, all of our credential elements fit on one line
                    $env_stream->write($env_file_line . "\n");
                }
            }
            foreach ($credentials_contents as $key => $value) {
                $env_stream->write("$key=\"$value\"\n");
            }
            $env_stream->end();
            if (!rename($tmp_file, $env_file)) {
                throw new \Exception("Failed to rename $env_file to $env_file");
            }
            $future->complete();
        } catch (\Exception $e) {
            if(is_file($tmp_file)) {
                unlink($tmp_file);
            }
            $future->error($e);
        }

        return $future->getFuture();
    }

    public function close(): void
    {
        EventLoop::cancel($this->signal);
    }
}
