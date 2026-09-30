<?php

namespace Massdriver;

use Amp\DeferredFuture;
use Amp\Future;
use Dotenv\Dotenv;
use Massdriver\AmpAws\AmpAws;
use Revolt\EventLoop;
use function Amp\async;
use function Amp\File\changeOwner;
use function Amp\File\changePermissions;
use function Amp\File\deleteFile;
use function Amp\File\getStatus;
use function Amp\File\isFile;
use function Amp\File\move;
use function Amp\File\read;
use function Amp\File\write;
use function Amp\File\touch;
use function Amp\File\listFiles;
use function Amp\Future\await;
use function Amp\Future\awaitAll;

function readFileContents(string $path): string
{
    if (!isFile($path)) {
        touch($path); // Preserve creation of missing tenant files.
    }
    return read($path);
}

function writeFileContents(string $path, string $contents, ?string $same_as_file = null): void
{
    if ($same_as_file !== null) {
        $status = getStatus($same_as_file);
        if ($status === null) {
            throw new \RuntimeException("Unable to read file metadata for {$same_as_file}");
        }
        // Apply permissions before writing credentials to the temporary file.
        touch($path);
        changeOwner($path, $status['uid'], $status['gid']);
        changePermissions($path, $status['mode'] & 07777);
    }
    write($path, $contents);
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

    public function __construct(
        public string $directory,
        public string $inline_role = '',
        public array $arns = [],
        public int $credential_duration = 129_600, //TODO - DUPLICATION!
        public int $refresh_threshold = 3600, //TODO - DUPLICATION!
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
        foreach(static::$refresh_credentials_timers as $timer) {
            // print "Timer for: $slug -> $timer\n";
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
            // print "looking at tenant: $tenant\n";
            $filename = $this->env_path_for_tenant($tenant);
            if (!isFile($filename)) {
                print "HOPEFULLY DEBUGGING ONLY - no `.env` file for $filename\n";
                // continue; In any kind of 'prod' environment, this wouldn't happen. But for testing it's at least useful
                // all that being said; let things roll (IMHO)
            }
            $futures[] = async(function () use ($filename, &$soonest, $tenant) { // parallelize each tenant's 'load'
                $env_file_contents = readFileContents($filename);
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
        // print "Single Credential refresh loop for $tenant (retry count: $retry_count)\n";
        $time_remaining_in_token = ($credentials['_SESSION_EXPIRATION'] ?? 0) - microtime(true);
        if ( $time_remaining_in_token < $this->refresh_threshold ) {
            if(self::$refresh_credentials_timers[$tenant] ?? false) {
                //determine if that timer is valid, and if so, cancel it?

                self::$refresh_credentials_timers[$tenant] = null;
            }
            try {
                $credential_future = $this->refresh_one_credential($tenant);
                $env_file_contents = async(fn () => readFileContents($this->env_path_for_tenant($tenant)));
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
        $delay_with_floor = max(0.1,$new_time_remaining_in_token - $this->refresh_threshold);
        $timer_id = EventLoop::delay($delay_with_floor, function () use ($tenant) {
            async(fn () => print "Timer for $tenant has fired!\n"); //weird, but necessary to avoid junking up the AWS calls?

            $this->single_credential_refresh_loop($tenant);
        });
        EventLoop::unreference($timer_id); //don't let timers keep the event loop up
        self::$refresh_credentials_timers[$tenant] = $timer_id;

}

    public function refresh_one_credential(string $tenant): Future
    {
        return async( function () use ($tenant) {
            $params = [
                'DurationSeconds' => $this->credential_duration,
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
        return async(function () use ($tenant, $credentials, $old_env_file): void {
            // Resolve inputs before creating a temporary file.
            [$env_file_contents, $credentials_contents] = await([$old_env_file, $credentials]);
            $env_file = $this->env_path_for_tenant($tenant);
            $tmp_file = $env_file . ".tmp";
            try {
                $updated_contents = '';
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
                        $updated_contents .= "$name=$value\n";
                    } else {
                        // 'literal' mode
                        // (works for comment-lines, blank-lines, and continuations of multi-line variables)
                        // Luckily, all of our credential elements fit on one line
                        $updated_contents .= $env_file_line . "\n";
                    }
                }
                foreach ($credentials_contents as $key => $value) {
                    $updated_contents .= "$key=\"$value\"\n";
                }
                writeFileContents($tmp_file, $updated_contents, $env_file);
                move($tmp_file, $env_file);
            } finally {
                if (isFile($tmp_file)) {
                    deleteFile($tmp_file);
                }
            }
        });
    }

    public function close(): void
    {
        EventLoop::cancel($this->signal);
    }
}
