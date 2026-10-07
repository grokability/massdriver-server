<?php

namespace Massdriver;

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
use function Amp\File\isDirectory;
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
        touch($path);
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

class FederatedClientCredentialsRefresher extends EventLoopTask
{
    const array CREDENTIAL_MAP = [
        'AWS_ACCESS_KEY_ID' => 'AccessKeyId',
        'AWS_SECRET_ACCESS_KEY' => 'SecretAccessKey',
        'AWS_SESSION_TOKEN' => 'SessionToken',
        '_SESSION_EXPIRATION' => 'Expiration',
    ];

    const string ENV_FILE_COMMENT = '# .env file modified by FederatedClientCredentialsRefresher, a part of Massdriver';

    public static function get_env_vars(): array
    {
        return [
            'directory' => 'DIRECTORY_OF_ENV_VARS',
            'inline_role' => [
                'INLINE_ROLE' => fn($inline_role) => $inline_role, // TODO, I'm on the fence on this one...,
                'INLINE_ROLE_QUOTES_TO_APOSTROPHES' => fn($e) => str_replace("'",'"',$e)
            ],
            'arns' => [
                'ROLE_ARNS' => fn($e) => array_filter(explode(",",$e)),
            ],
            'credential_duration' => 'CREDENTIAL_DESIRED_DURATION',
            'refresh_threshold' => 'CREDENTIAL_REFRESH_THRESHOLD'
        ];
    }

    protected int $refreshes = 0;
    protected array $credentials_array = [];
    protected array $refresh_credentials_timers = [];
    protected ?Future $reload = null;
    protected bool $stopping = false;
    protected array $refreshing = [];
    protected array $retry_counts = [];
    protected AmpAws $sts_client;
    protected int $number_of_tenants = 0;

    public function __construct(
        /** @noinspection SpellCheckingInspection */
        public string $directory,
        public string $inline_role = '',
        public array $arns = [],
        public int $credential_duration = 129_600,
        public int $refresh_threshold = 3600,
    )
    {
        print "Constructing Refresher Object\n";
        $this->sts_client = new AmpAws(
            'Sts',
            [
                'version' => '2011-06-15',
            ]
        );
    }

    public function __invoke(): void
    {
        $this->reload();
    }

    function graceful_shutdown(): void
    {
        print "ClientCredentials refresher is gracefully shutting down\n";
        $this->stopping = true;
        foreach ($this->refresh_credentials_timers as $timer) {
            EventLoop::cancel($timer);
        }
        $this->refresh_credentials_timers = [];
    }

    public function reload(): void
    {
        $this->load_credentials_from_disk()->catch(static function (\Throwable $error): void {
            print "Unable to load tenant credentials: {$error->getMessage()}\n";
        });
    }

    public function env_path_for_tenant($tenant): string
    {
        return rtrim($this->directory, '/') . "/$tenant/.env";
    }

    public function load_credentials_from_disk(): Future
    {
        if ($this->stopping) {
            return Future::complete();
        }
        if ($this->reload !== null && !$this->reload->isComplete()) {
            return $this->reload;
        }
        return $this->reload = async(function (): void {
            $futures = [];
            foreach (listFiles($this->directory) as $tenant) {
                if (!isDirectory(rtrim($this->directory, '/') . '/' . $tenant)) {
                    continue;
                }
                $futures[$tenant] = async(function () use ($tenant): void {
                    // A reload must not overwrite state halfway through a renewal.
                    if (isset($this->refreshing[$tenant])) {
                        $this->refreshing[$tenant]->await();
                    }
                    if ($this->stopping) {
                        return;
                    }
                    $contents = readFileContents($this->env_path_for_tenant($tenant));
                    $this->credentials_array[$tenant] = array_intersect_key(Dotenv::parse($contents), self::CREDENTIAL_MAP);
                    $this->single_credential_refresh_loop($tenant);
                });
            }
            $this->number_of_tenants=count($futures);

            // Settle every tenant before reporting failures, so reloads cannot overlap.
            [$errors, $_values] = awaitAll($futures);
            if ($errors) {
                $failed_tenants = array_keys($errors);
                $first_failed_tenant = array_key_first($failed_tenants);
                throw new \RuntimeException("Failed to load tenants: ".implode(",",$failed_tenants) .". First Failed Tenant: $first_failed_tenant - ". $errors[$first_failed_tenant]->getMessage(), 0, $errors[$first_failed_tenant]);
            }
        });
    }

    public function single_credential_refresh_loop(string $tenant): void
    {
        if ($this->stopping) {
            return;
        }
        if (isset($this->refreshing[$tenant])) {
            $this->refreshing[$tenant]->await();
            return;
        }
        if (isset($this->refresh_credentials_timers[$tenant])) {
            EventLoop::cancel($this->refresh_credentials_timers[$tenant]);
            unset($this->refresh_credentials_timers[$tenant]);
        }
        // I hate this, but I don't know exactly how else to do it? I hate the multiple-assignments, same-line thing
        $refresh = $this->refreshing[$tenant] = async(function () use ($tenant): void {
            $credentials = $this->credentials_array[$tenant];
            $delay = ($credentials['_SESSION_EXPIRATION'] ?? 0) - microtime(true) - $this->refresh_threshold;
            if ($delay <= 0) {
                try {
                    $credential_future = async(fn () => $this->refresh_one_credential($tenant));
                    $contents = async(fn () => readFileContents($this->env_path_for_tenant($tenant)));
                    $this->write_one_credential($tenant, $credential_future, $contents);
                    $credentials = $credential_future->await();
                    $this->credentials_array[$tenant] = $credentials;
                    unset($this->retry_counts[$tenant]);
                    $delay = $credentials['_SESSION_EXPIRATION'] - microtime(true) - $this->refresh_threshold;
                } catch (\Throwable $error) {
                    $delays = [5, 10, 30, 45, 60];
                    $attempt = $this->retry_counts[$tenant] ?? 0;
                    $delay = $delays[min($attempt, count($delays) - 1)];
                    $this->retry_counts[$tenant] = min($attempt + 1, count($delays) - 1);
                    print "Exception caught when renewing credentials: $tenant: {$error->getMessage()}\n";
                }
            }
            if (!$this->stopping) {
                $timer = EventLoop::delay(max(0.1, $delay), function () use ($tenant): void {
                    unset($this->refresh_credentials_timers[$tenant]);
                    $this->single_credential_refresh_loop($tenant);
                });
                EventLoop::unreference($timer);
                $this->refresh_credentials_timers[$tenant] = $timer;
            }
        });
        try {
            $refresh->await();
        } finally {
            unset($this->refreshing[$tenant]);
        }
    }

    public function refresh_one_credential(string $tenant): array
    {
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
        $result = $this->sts_client->getFederationToken($params);
        $credentials = [];
        $results = $result['Credentials'];
        foreach(self::CREDENTIAL_MAP as $env_name => $aws_name) {
            if(array_key_exists($aws_name, $results)) {
                $credentials[$env_name] = $results[$aws_name];
            }
        }
        $credentials['_SESSION_EXPIRATION'] = strtotime($credentials['_SESSION_EXPIRATION']) ?: 0;
        return $credentials;
    }

    public function write_one_credential(string $tenant, Future $credentials, Future $old_env_file):void
    {
        $this->refreshes++;
        // Resolve inputs before creating a temporary file.
        [$env_file_contents, $credentials_contents] = await([$old_env_file, $credentials]);
        $env_file = $this->env_path_for_tenant($tenant);
        $tmp_file = $env_file . ".tmp";
        try {
            // First, 'strip out' any credentials lines. They're all guaranteed to be in exactly *one* line
            // Double-escape newline tokens so PHP does not turn them into whitespace which is ignored by the "/x" option.
            foreach(array_keys(self::CREDENTIAL_MAP) as $aws_name) {
                $pattern = "/^ # start-of-line
                $aws_name= # the variable we want in question, *and* its equals-sign
                [^\\n]* # a bunch of non-newline characters
                
                (?:\\n|\\z) # a non-capturing group, of either a *newline*, or end-of-file
                # this needs to be run in multiline mode (m), with PCRE_EXTENDED (x) enabled
                /mx";
                $env_file_contents = preg_replace($pattern,"",$env_file_contents);
            }

            // next, strip out the comment that we append (so we don't keep appending it over and over and over...)
            $env_file_contents = preg_replace("/^".static::ENV_FILE_COMMENT."(?:\\n|\\z)/m","",$env_file_contents);

            // (side-note: if the file didn't have a 'trailing newline' at the end, and we delete the last line,
            // then it definitely *does* now. That's fine.)

            // but if remaining content *didn't* have a trailing newline, make sure we get one.
            if ($env_file_contents !== '' && $env_file_contents[-1] != "\n") {
                $env_file_contents.="\n";
            }
            // Then, append the new credentials to the bottom after a comment
            $env_file_contents.=static::ENV_FILE_COMMENT."\n";
            foreach ($credentials_contents as $key => $value) {
                $env_file_contents .= "$key=\"$value\"\n";
            }
            writeFileContents($tmp_file, $env_file_contents, $env_file);
            move($tmp_file, $env_file);
        } finally {
            if (isFile($tmp_file)) {
                deleteFile($tmp_file);
            }
        }
    }

    public function get_iterations_count(): int
    {
        // We figure an "iteration" is a full cycle of refreshes, one for every tenant
        // hence the math below
        return $this->number_of_tenants === 0 ? 0 : (int)floor($this->refreshes / $this->number_of_tenants);
    }
}
