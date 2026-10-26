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

class TenantCredentialsRefresher extends EventLoopTask
{
    const array CREDENTIAL_MAP = [
        'AWS_ACCESS_KEY_ID' => 'AccessKeyId',
        'AWS_SECRET_ACCESS_KEY' => 'SecretAccessKey',
        'AWS_SESSION_TOKEN' => 'SessionToken',
        '_SESSION_EXPIRATION' => 'Expiration',
    ];

    const string ENV_FILE_COMMENT = '# .env file modified by TenantCredentialsRefresher, a part of Massdriver';

    public static function get_env_vars(): array
    {
        return [
            'directory' => 'DIRECTORY_OF_ENV_VARS',
            'inline_role' => [
                'INLINE_ROLE' => fn($inline_role) => $inline_role, // TODO, I'm on the fence on this one...,
                'INLINE_ROLE_QUOTES_TO_APOSTROPHES' => fn($e) => str_replace("'",'"',$e)
            ],
            'arns' => [
                'POLICY_ARNS' => fn($e) => array_filter(explode(",",$e)),
            ],
            'credential_duration' => 'CREDENTIAL_DESIRED_DURATION',
            'refresh_threshold' => 'CREDENTIAL_REFRESH_THRESHOLD',
            'assume_role_arn' => 'ASSUME_ROLE_ARN',
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
    protected string $directory_prefix;
    protected string $directory_suffix;
    protected string $sts_command;

    public function __construct(
        /** @noinspection SpellCheckingInspection */
        string $directory,
        public string $inline_role = '',
        public array $arns = [],
        public string $assume_role_arn='',
        public int $credential_duration = 129_600,
        public int $refresh_threshold = 3600,
    )
    {
        print "Constructing Refresher Object\n";
        if($this->assume_role_arn) {
            $this->sts_command = 'assumeRole';
        } else {
            $this->sts_command = 'getFederationToken';
        }
        print "Selected Credential refresh method: ".$this->sts_command."\n";
        $this->sts_client = new AmpAws(
            'Sts',
            [
                'version' => '2011-06-15',
            ]
        );
        if(!str_contains($directory,'{TENANT}')){
            throw new \Exception("Directory '$directory' does not have {TENANT}");
        }
        list($this->directory_prefix, $this->directory_suffix) = explode('{TENANT}',$directory); //TODO - what if {TENANT} repeats?
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

    public function hard_reload(): void
    {
        print "Forcibly re-freshing *all* client credentials\n";
        $this->load_credentials_from_disk(true)->catch(static function (\Throwable $error): void {
            print "Unable to load tenant credentials: {$error->getMessage()}\n";
        });
    }

    public function env_path_for_tenant($tenant): string
    {
        return rtrim(rtrim($this->directory_prefix, '/') . "/$tenant/".$this->directory_suffix,'/')."/.env";
    }

    public function load_credentials_from_disk(bool $force_reload = false): Future
    {
        if ($this->stopping) {
            return Future::complete();
        }
        if ($this->reload !== null && !$this->reload->isComplete()) {
            return $this->reload;
        }
        $this->reload = async(function () use($force_reload): void {
            $futures = [];
            foreach (listFiles($this->directory_prefix) as $tenant) {
                $dirname = rtrim($this->directory_prefix, '/') . "/$tenant";
                if (!isDirectory($dirname)) {
                    print "Warning: $dirname is not a directory\n";
                    continue;
                }
                $futures[$tenant] = async(function () use ($tenant,$force_reload): float {
                    // A reload must not overwrite state halfway through a renewal.
                    if (isset($this->refreshing[$tenant])) {
                        $this->refreshing[$tenant]->await();
                    }
                    if ($this->stopping) {
                        return -1;
                    }
                    $start = microtime(true);
                    $this->single_credential_refresh_loop($tenant,$force_reload);
                    return $start;
                })->catch(function (\Throwable $error) use ($tenant): void {
                    print "Tenant $tenant credential refresh *failed* - ".$error->getMessage()."\n";
                });
            }
            //now, find directories that have *disappeared* and cancel out their timers and stuff.
            $known_environments = $this->credentials_array + $this->refresh_credentials_timers
                + $this->refreshing + $this->retry_counts;
            $removed_environments = array_diff_key($known_environments,$futures);
            $refreshing_removed_environments = array_intersect_key($this->refreshing, $removed_environments);
            //just let a credential finish refreshing on a removed environment; canceling won't save us very much.
            awaitAll($refreshing_removed_environments);
            foreach($removed_environments as $tenant => $credentials) {
                if (isset($this->refresh_credentials_timers[$tenant])) {
                    EventLoop::cancel($this->refresh_credentials_timers[$tenant]);
                    unset($this->refresh_credentials_timers[$tenant]);
                    print "Canceling timer for $tenant because it's been removed, and ";
                }
                print "Deleting credentials entry for $tenant\n";
                unset($this->credentials_array[$tenant], $this->retry_counts[$tenant]);
            }
            $this->number_of_tenants = count($futures);
            print "Number of tenants has been initialized to: ".$this->number_of_tenants."\n";
        });
        return $this->reload;
    }

    public function single_credential_refresh_loop(string $tenant,bool $force_reload=false): void
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
        $this->refreshing[$tenant] = async(function () use ($tenant,$force_reload): void {
            $start = microtime(true);
            try {
                if(!array_key_exists($tenant,$this->credentials_array) && !$force_reload) {
                    // in this case, credentials *might* be on the disk, but are definitely *not* in the credentials-cache.
                    // Re-reading them from disk prevents us from refreshing perfectly good credentials unnecessarily.
                    // But there's no point in doing this if we're doing a "Force reload" - we're just going to
                    // snag new credentials anyways, so why bother looking up what they might be now?
                    $env_file_contents = readFileContents($this->env_path_for_tenant($tenant));
                    if($env_file_contents) {
                        $env_vars = Dotenv::parse($env_file_contents);
                        $this->credentials_array[$tenant] = array_intersect_key($env_vars,static::CREDENTIAL_MAP);
                        print "Loaded $tenant credentials from disk!\n";
                    } else {
                        print "No credentials found for $tenant\n";
                    }
                }
                $credentials = $this->credentials_array[$tenant] ?? [];
                $delay = ($credentials['_SESSION_EXPIRATION'] ?? 0) - microtime(true) - $this->refresh_threshold;
                if ($delay <= 0 || $force_reload) {
                    $credential_future = async(fn () => $this->refresh_one_credential($tenant));
                    $contents = async(fn () => readFileContents($this->env_path_for_tenant($tenant)));
                    $this->write_one_credential($tenant, $credential_future, $contents);
                    $credentials = $credential_future->await();
                    $this->credentials_array[$tenant] = $credentials;
                    unset($this->retry_counts[$tenant]);
                    $delay = $credentials['_SESSION_EXPIRATION'] - microtime(true) - $this->refresh_threshold;
                    print "Actual re-fetch of credentials for $tenant in IAM took ".(microtime(true)-$start)." seconds, next scheduled for $delay seconds\n";
                } else {
                    //confirm that the credentials are actually on the disk - in some cases, they're in memory, but not on the disk!
                    $credentials_on_disk = true;
                    $credentials_contents = readFileContents($this->env_path_for_tenant($tenant));
                    foreach(array_keys(static::CREDENTIAL_MAP) as $env_var_name) {
                        if(!strstr($credentials_contents, $env_var_name)) {
                            $credentials_on_disk = false;
                            break;
                        }
                    }
                    if(!$credentials_on_disk) {
                        print "Credentials for $tenant are not present in .env file, rewriting them. Refreshes in: $delay seconds.\n";
                        // it's a little weird that we have the tenants credentials in *memory*, but we have to
                        // wrap it in an async() - but that's the signature of that function; it wants a Future
                        $this->write_one_credential($tenant, async(fn () => $this->credentials_array[$tenant]), async(fn () => readFileContents($this->env_path_for_tenant($tenant))));
                    } else {
                        print "Credentials not due for refresh for $tenant yet, still need $delay seconds\n";
                    }
                }
            } catch (\Throwable $error) {
                // TODO: I don't get why the math is so weird here. What's up with that?
                $delays = [5, 10, 30, 45, 60];
                $attempt = $this->retry_counts[$tenant] ?? 0;
                $delay = $delays[min($attempt, count($delays) - 1)];
                $this->retry_counts[$tenant] = min($attempt + 1, count($delays) - 1);
                print "Exception caught when renewing credentials: $tenant: {$error->getMessage()}. Retrying in $delay seconds.\n";
                // this *executes* on its delay by falling off the end here, and getting handled by the
                // EventLoop::delay() stuff, below
            }
            if($this->stopping) {
                // Do not try and schedule a new refresh if we're stopping
                return;
            }
            $timer = EventLoop::delay(max(0.1, $delay), function () use ($tenant): void {
                unset($this->refresh_credentials_timers[$tenant]);
                $this->single_credential_refresh_loop($tenant);
            });
            EventLoop::unreference($timer); //credential refreshes should not be enough to hold the event-loop open
            $this->refresh_credentials_timers[$tenant] = $timer;
        })->finally(function () use ($tenant) { unset($this->refreshing[$tenant]);});
    }

    public function refresh_one_credential(string $tenant): array
    {
        $params = [
            'DurationSeconds' => $this->credential_duration,
            'Tags' => [
                [
                    'Key' => 'slug',
                    'Value' => $tenant,
                ]
            ]
        ];
        $params+= match($this->sts_command) {
            'getFederationToken' => [
                'Name' => md5($tenant),
            ],
            'assumeRole' => [
                'RoleSessionName' => $tenant,
                'RoleArn' => $this->assume_role_arn
            ],
            default => throw new \RuntimeException("Unsupported STS method: " . $this->sts_command)
        };
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
        $result = $this->sts_client->{$this->sts_command}($params);
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
        // print "DEBUG: raw iterations are: ".$this->refreshes." vs tenant count of ".$this->number_of_tenants."\n";
        return $this->number_of_tenants === 0 ? -1 : (int)floor($this->refreshes / $this->number_of_tenants);
    }
}
