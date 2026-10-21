<?php
declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

use Amp\DeferredFuture;
use Amp\Future;
use Aws\Api\DateTimeResult;
use Aws\Result;
use Dotenv\Dotenv;
use Massdriver\AmpAws\AmpAws;
use Massdriver\FederatedClientCredentialsRefresher as Refresher;
use Revolt\EventLoop;
use function Amp\async;
use function Amp\delay;

\Amp\File\filesystem(new \Amp\File\Driver\BlockingFilesystemDriver());

function check(bool $condition, string $message): void
{
    if (!$condition) { throw new RuntimeException($message); }
}

final class FakeSts extends AmpAws
{
    public array $calls = [];
    public ?Throwable $failure = null;
    public ?DeferredFuture $started = null;
    public ?DeferredFuture $release = null;
    public function __construct() {}
    public function __call(string $name, array $arguments): Result
    {
        check($name === 'getFederationToken', 'Unexpected STS operation');
        $this->calls[] = $arguments[0];
        $this->started?->complete();
        $this->release?->getFuture()->await();
        if ($this->failure !== null) { throw $this->failure; }
        return new Result(['Credentials' => [
            'AccessKeyId' => 'test-key',
            'SecretAccessKey' => 'test-secret',
            'SessionToken' => 'test-token/+=',
            'Expiration' => new DateTimeResult('2035-01-01T00:00:00Z'),
        ]]);
    }
}

function state(Refresher $refresher, string $property): mixed
{
    return (new ReflectionProperty(Refresher::class, $property))->getValue($refresher);
}

function seed(Refresher $refresher, string $tenant, float $expiration): void
{
    $credentials = state($refresher, 'credentials_array');
    $credentials[$tenant] = ['_SESSION_EXPIRATION' => $expiration];
    (new ReflectionProperty(Refresher::class, 'credentials_array'))->setValue($refresher, $credentials);
}

function liveTimers(Refresher $refresher): array
{
    return array_values(array_intersect(
        array_filter(state($refresher, 'refresh_credentials_timers')),
        EventLoop::getIdentifiers(),
    ));
}

function refreshAndWait(Refresher $refresher, string $tenant, bool $force = false): void
{
    $refresher->single_credential_refresh_loop($tenant, $force);
    $pending = state($refresher, 'refreshing');
    $future = $pending[$tenant] ?? null;
    $future?->await();
}

function loadAndWait(Refresher $refresher, bool $force = false): void
{
    $refresher->load_credentials_from_disk($force)->await();
    // Loading starts background tenant work. Drain it before asserting, while
    // unreferenced renewal timers remain scheduled for their future deadlines.
    EventLoop::run();
    check(state($refresher, 'refreshing') === [], 'Tenant work did not finish');
}

function assertRemoved(Refresher $refresher, string $tenant, string $timer): void
{
    check(!array_key_exists($tenant, state($refresher, 'credentials_array')), 'Removed tenant retained cached credentials');
    check(!array_key_exists($tenant, state($refresher, 'refresh_credentials_timers')), 'Removed tenant retained a timer entry');
    check(!in_array($timer, EventLoop::getIdentifiers(), true), 'Removed tenant timer was not actually cancelled');
    check(!array_key_exists($tenant, state($refresher, 'refreshing')), 'Removed tenant retained active refresh work');
    check(!array_key_exists($tenant, state($refresher, 'retry_counts')), 'Removed tenant retained its retry count');
}

// Construct without the real STS client so no credential discovery or network
// access is possible. All loading, scheduling, mapping and writing uses production methods.
function fixture(string $directory, FakeSts $sts): Refresher
{
    $reflection = new ReflectionClass(Refresher::class);
    $refresher = $reflection->newInstanceWithoutConstructor();
    $reflection->getProperty('directory_prefix')->setValue($refresher, $directory);
    $reflection->getProperty('directory_suffix')->setValue($refresher, '');
    $refresher->inline_role = '{"Version":"2012-10-17","Statement":[]}';
    $refresher->arns = ['arn:aws:iam::123456789012:policy/test'];
    $refresher->credential_duration = 129_600;
    $refresher->refresh_threshold = 3600;
    $reflection->getProperty('sts_client')->setValue($refresher, $sts);
    return $refresher;
}

$directory = sys_get_temp_dir().'/massdriver-tests-'.bin2hex(random_bytes(8));
mkdir($directory.'/tenant', 0700, true);
$path = $directory.'/tenant/.env';
$original = "# keep this comment\n\nAPP_NAME=example\nAWS_ACCESS_KEY_ID=old-key\nAWS_SECRET_ACCESS_KEY=old-secret\nAWS_SESSION_TOKEN=old-token\n_SESSION_EXPIRATION=2208988800\n";
file_put_contents($path, $original);
chmod($path, 0600);
$sts = new FakeSts();
$refresher = null;
$cases = [
    'mapping' => static function () use (&$refresher, $sts): void {
        $credentials = $refresher->refresh_one_credential('tenant');
        check($credentials === [
            'AWS_ACCESS_KEY_ID' => 'test-key', 'AWS_SECRET_ACCESS_KEY' => 'test-secret',
            'AWS_SESSION_TOKEN' => 'test-token/+=', '_SESSION_EXPIRATION' => 2051222400,
        ], 'STS fields or SDK expiration were mapped incorrectly');
        check($sts->calls[0]['Policy'] === $refresher->inline_role, 'Inline policy was lost');
        check($sts->calls[0]['PolicyArns'] === [['arn' => $refresher->arns[0]]], 'Policy ARN shape is wrong');
        check($sts->calls[0]['Tags'] === [['Key' => 'slug', 'Value' => 'tenant']], 'Tenant tag was lost');
    },
    'write-success' => static function () use (&$refresher, $path, $original): void {
        $before = stat($path);
        $credentials = async($refresher->refresh_one_credential(...), 'tenant');
        $refresher->write_one_credential('tenant', $credentials, Future::complete($original));
        $contents = file_get_contents($path);
        $parsed = Dotenv::parse($contents);
        check($parsed['AWS_ACCESS_KEY_ID'] === 'test-key' && $parsed['AWS_SESSION_TOKEN'] === 'test-token/+=', 'Credentials not replaced/appended');
        check($parsed['APP_NAME'] === 'example' && str_starts_with($contents, "# keep this comment\n\n"), 'Unrelated content changed');
        clearstatcache(true, $path);
        $after = stat($path);
        foreach (['uid', 'gid', 'mode'] as $field) { check($before[$field] === $after[$field], 'File metadata changed: '.$field); }
        check(!file_exists($path.'.tmp'), 'Temporary file remained after success');
    },
    'write-failure' => static function () use (&$refresher, $path, $original): void {
        $failure = new RuntimeException('simulated STS rejection');
        try {
            $refresher->write_one_credential('tenant', Future::error($failure), Future::complete($original));
            throw new RuntimeException('Failed credentials unexpectedly succeeded');
        } catch (Throwable $error) { check($error === $failure, 'Original failure was lost'); }
        EventLoop::run(); // Detect delayed close callbacks overwriting the file.
        check(file_get_contents($path) === $original, 'Failed renewal changed the original file');
        check(!file_exists($path.'.tmp'), 'Failed renewal left a temporary file');
    },
    'write-error-cleanup' => static function () use (&$refresher, $path, $original): void {
        $failure = new TypeError('simulated credential transformation error');
        try {
            $refresher->write_one_credential('tenant', Future::error($failure), Future::complete($original));
            throw new RuntimeException('Error unexpectedly succeeded');
        } catch (Throwable $error) { check($error === $failure, 'Original Error was lost'); }
        check(file_get_contents($path) === $original, 'Error changed the original file');
        check(!file_exists($path.'.tmp'), 'TypeError bypassed temporary-file cleanup');
    },
    'duplicate-keys' => static function () use (&$refresher, $path): void {
        $contents = "export AWS_ACCESS_KEY_ID=first\n\"AWS_ACCESS_KEY_ID\"=second\n";
        $refresher->write_one_credential('tenant', Future::complete(['AWS_ACCESS_KEY_ID' => 'new']), Future::complete($contents));
        check(Dotenv::parse(file_get_contents($path))['AWS_ACCESS_KEY_ID'] === 'new', 'Later duplicate overrode the refreshed credential');
    },
    'concurrent-refresh' => static function () use (&$refresher, $sts): void {
        seed($refresher, 'tenant', 0);
        $sts->started = new DeferredFuture();
        $sts->release = new DeferredFuture();
        $first = async(fn () => $refresher->single_credential_refresh_loop('tenant'));
        $sts->started->getFuture()->await();
        $second = async(fn () => $refresher->single_credential_refresh_loop('tenant'));
        $reload = $refresher->load_credentials_from_disk();
        delay(0.01);
        check(count($sts->calls) === 1, 'Overlapping renewal called STS twice');
        $sts->release->complete();
        \Amp\Future\await([$first, $second, $reload]);
        EventLoop::run();
        check(count($sts->calls) === 1 && count(liveTimers($refresher)) === 1, 'Reload raced with renewal or duplicated its timer');
    },
    'retry-reset' => static function () use (&$refresher, $sts): void {
        seed($refresher, 'tenant', 0);
        $sts->failure = new RuntimeException('outage');
        refreshAndWait($refresher, 'tenant');
        refreshAndWait($refresher, 'tenant');
        $sts->failure = null;
        refreshAndWait($refresher, 'tenant');
        check(count($sts->calls) === 3, 'Retry test did not execute all three attempts');
        check(state($refresher, 'retry_counts') === [], 'Successful renewal retained its backoff');
    },
    'load' => static function () use (&$refresher, $sts, $path, $original): void {
        loadAndWait($refresher);
        check(state($refresher, 'credentials_array')['tenant']['AWS_ACCESS_KEY_ID'] === 'old-key', 'Tenant credentials were not loaded');
        check($sts->calls === [], 'Unexpired credentials were unnecessarily renewed');
        check(count(liveTimers($refresher)) === 1, 'Tenant renewal was not scheduled');
        check(file_get_contents($path) === $original, 'Valid startup credentials were rewritten');
        check(!isset(state($refresher, 'credentials_array')['tenant']['APP_NAME']), 'Unrelated environment values entered the credential cache');
        $refresher->graceful_shutdown();
        check(liveTimers($refresher) === [], 'Shutdown left renewal timers');
        EventLoop::run();
    },
    'backoff' => static function () use (&$refresher, $sts): void {
        seed($refresher, 'tenant', 0);
        $sts->failure = new RuntimeException('simulated STS outage');
        refreshAndWait($refresher, 'tenant');
        delay(0.35); // Much shorter than the intended first retry delay of five seconds.
        check(count($sts->calls) === 1, 'Credential failure retried within 350ms instead of backing off');
    },
    'retry-isolation' => static function () use (&$refresher, $sts, $directory): void {
        $sts->failure = new RuntimeException('simulated STS outage');
        for ($i = 0; $i < 6; $i++) {
            $tenant = 'tenant'.$i;
            mkdir($directory.'/'.$tenant, 0700);
            file_put_contents($directory.'/'.$tenant.'/.env', '_SESSION_EXPIRATION=0');
            seed($refresher, $tenant, 0);
            try { refreshAndWait($refresher, $tenant); }
            catch (Throwable $error) { throw new RuntimeException('One tenant exhausted another tenant\'s retry budget', 0, $error); }
        }
    },
    'reschedule' => static function () use (&$refresher, $path): void {
        seed($refresher, 'tenant', microtime(true) + $refresher->refresh_threshold + 0.1);
        refreshAndWait($refresher, 'tenant');
        // Another actor refreshes credentials before our earlier timer fires.
        file_put_contents($path, '_SESSION_EXPIRATION='.(time() + 7200));
        loadAndWait($refresher);
        delay(0.3);
        check(count(liveTimers($refresher)) === 1, 'Reload left a stale timer ID and no future renewal');
    },
    'shutdown-in-flight' => static function () use (&$refresher, $sts): void {
        seed($refresher, 'tenant', 0);
        $sts->started = new DeferredFuture();
        $sts->release = new DeferredFuture();
        $refresh = async(fn () => $refresher->single_credential_refresh_loop('tenant'));
        $sts->started->getFuture()->await();
        $refresher->graceful_shutdown();
        $sts->release->complete();
        $refresh->await();
        EventLoop::run();
        check(liveTimers($refresher) === [], 'In-flight renewal scheduled another timer after shutdown');
    },
    'force-reload' => static function () use (&$refresher, $sts, $path): void {
        loadAndWait($refresher);
        $oldTimer = state($refresher, 'refresh_credentials_timers')['tenant'];
        loadAndWait($refresher, true);
        check(count($sts->calls) === 1, 'Forced reload did not renew valid credentials exactly once');
        check(Dotenv::parse(file_get_contents($path))['AWS_ACCESS_KEY_ID'] === 'test-key', 'Forced reload did not replace disk credentials');
        check(state($refresher, 'credentials_array')['tenant']['AWS_ACCESS_KEY_ID'] === 'test-key', 'Forced reload did not update the cache');
        check(count(liveTimers($refresher)) === 1, 'Forced reload did not schedule exactly one renewal');
        check(!in_array($oldTimer, EventLoop::getIdentifiers(), true), 'Forced reload retained the previous timer');
    },
    'malformed-env' => static function () use (&$refresher, $sts, $path, $directory, $original): void {
        $malformed = "=invalid-name\n";
        file_put_contents($path, $malformed);
        mkdir($directory.'/healthy', 0700);
        file_put_contents($directory.'/healthy/.env', $original);
        // Reject invalid dotenv content without overwriting it or taking down
        // the event loop; another healthy tenant must still load and schedule.
        loadAndWait($refresher);
        check(file_get_contents($path) === $malformed, 'Malformed tenant configuration was overwritten');
        check($sts->calls === [], 'Malformed configuration unexpectedly requested new credentials');
        check(state($refresher, 'credentials_array')['healthy']['AWS_ACCESS_KEY_ID'] === 'old-key', 'Malformed tenant prevented healthy tenant loading');
        check(isset(state($refresher, 'refresh_credentials_timers')['healthy']), 'Healthy tenant renewal was not scheduled');
        check(count(liveTimers($refresher)) === 2, 'Malformed tenant did not retain a retry alongside the healthy renewal');
        check(state($refresher, 'retry_counts')['tenant'] === 1, 'Malformed tenant did not enter failure backoff');
        // Simulate the scheduled retry after an operator fixes the file,
        // without waiting five seconds for its timer.
        file_put_contents($path, $original);
        refreshAndWait($refresher, 'tenant');
        check(state($refresher, 'credentials_array')['tenant']['AWS_ACCESS_KEY_ID'] === 'old-key', 'Corrected tenant did not recover on retry');
        check(count(liveTimers($refresher)) === 2, 'Recovered tenant lost or duplicated a renewal timer');
        check($sts->calls === [], 'Corrected valid credentials were unnecessarily refreshed');
    },
    'removed-tenant' => static function () use (&$refresher, $sts, $path, $directory, $original): void {
        mkdir($directory.'/healthy', 0700);
        file_put_contents($directory.'/healthy/.env', $original);
        loadAndWait($refresher);
        $oldTimer = state($refresher, 'refresh_credentials_timers')['tenant'];
        unlink($path);
        rmdir($directory.'/tenant');
        loadAndWait($refresher);
        assertRemoved($refresher, 'tenant', $oldTimer);
        check(state($refresher, 'number_of_tenants') === 1, 'Removed tenant was still counted');
        check(state($refresher, 'credentials_array')['healthy']['AWS_ACCESS_KEY_ID'] === 'old-key', 'Removal affected healthy credentials');
        check(count(liveTimers($refresher)) === 1, 'Removal affected the healthy tenant renewal');
        check($sts->calls === [], 'Removal unnecessarily refreshed healthy credentials');
    },
    'removed-in-flight' => static function () use (&$refresher, $sts, $path, $directory): void {
        loadAndWait($refresher);
        $oldTimer = state($refresher, 'refresh_credentials_timers')['tenant'];
        $sts->started = new DeferredFuture();
        $sts->release = new DeferredFuture();
        $refresher->single_credential_refresh_loop('tenant', true);
        $refresh = state($refresher, 'refreshing')['tenant'];
        $sts->started->getFuture()->await();
        unlink($path);
        rmdir($directory.'/tenant');
        $reload = $refresher->load_credentials_from_disk();
        // A queued barrier lets reload reach its wait on the gated STS call.
        async(static fn () => null)->await();
        check(!$reload->isComplete(), 'Removal did not wait for the active renewal');
        $sts->release->complete();
        \Amp\Future\await([$refresh, $reload]);
        EventLoop::run();
        assertRemoved($refresher, 'tenant', $oldTimer);
        check(liveTimers($refresher) === [], 'Failed in-flight write left a retry timer after removal');
        check(state($refresher, 'number_of_tenants') === 0, 'Last removed tenant was still counted');
        check(!is_dir($directory.'/tenant'), 'Renewal recreated a removed tenant directory');
        check(count($sts->calls) === 1, 'Removal launched another STS request');
    },
    'removed-uncached-retry' => static function () use (&$refresher, $sts, $path, $directory): void {
        file_put_contents($path, '');
        $sts->failure = new RuntimeException('first renewal failed');
        refreshAndWait($refresher, 'tenant');
        check(!isset(state($refresher, 'credentials_array')['tenant']), 'Fixture unexpectedly cached credentials after first failure');
        $oldTimer = state($refresher, 'refresh_credentials_timers')['tenant'];
        check(in_array($oldTimer, liveTimers($refresher), true), 'Failed renewal did not schedule a retry');
        unlink($path);
        rmdir($directory.'/tenant');
        loadAndWait($refresher);
        assertRemoved($refresher, 'tenant', $oldTimer);
        check(state($refresher, 'number_of_tenants') === 0, 'Uncached removed tenant was still counted');
        check(count($sts->calls) === 1, 'Removal launched another STS request');
    },
    'tenant-reappears' => static function () use (&$refresher, $sts, $path, $directory, $original): void {
        loadAndWait($refresher);
        $oldTimer = state($refresher, 'refresh_credentials_timers')['tenant'];
        unlink($path);
        rmdir($directory.'/tenant');
        loadAndWait($refresher);
        assertRemoved($refresher, 'tenant', $oldTimer);
        mkdir($directory.'/tenant', 0700);
        $replacement = str_replace('old-key', 'replacement-key', $original);
        file_put_contents($path, $replacement);
        loadAndWait($refresher);
        check(state($refresher, 'credentials_array')['tenant']['AWS_ACCESS_KEY_ID'] === 'replacement-key', 'Reappearing tenant reused stale credentials');
        check(file_get_contents($path) === $replacement, 'Reappearing tenant credentials were rewritten');
        check(count(liveTimers($refresher)) === 1, 'Reappearing tenant did not get exactly one renewal');
        check(state($refresher, 'number_of_tenants') === 1, 'Reappearing tenant was not counted');
        check($sts->calls === [], 'Reappearing tenant unnecessarily requested credentials');
    },
];

foreach (['expired', 'near-expiry', 'at-threshold', 'missing-env', 'empty-env', 'missing-expiration'] as $startupCase) {
    $cases['startup-'.$startupCase] = static function () use (&$refresher, $sts, $path, $original, $startupCase): void {
        $expiration = match ($startupCase) {
            'expired' => time() - 60,
            'near-expiry' => time() + $refresher->refresh_threshold - 60,
            // Capturing the boundary immediately before loading avoids sleeps;
            // by the eligibility check it is at or just inside the threshold.
            'at-threshold' => microtime(true) + $refresher->refresh_threshold,
            default => null,
        };
        if ($startupCase === 'missing-env') {
            unlink($path);
        } elseif ($startupCase === 'empty-env') {
            file_put_contents($path, '');
        } else {
            $contents = preg_replace('/^_SESSION_EXPIRATION=.*\n/m', '', $original);
            if ($expiration !== null) {
                $contents .= '_SESSION_EXPIRATION='.$expiration."\n";
            }
            file_put_contents($path, $contents);
        }
        loadAndWait($refresher);
        check(count($sts->calls) === 1, 'Startup did not request credentials exactly once: '.$startupCase);
        $credentials = state($refresher, 'credentials_array')['tenant'];
        $disk = Dotenv::parse(file_get_contents($path));
        foreach (['AWS_ACCESS_KEY_ID' => 'test-key', 'AWS_SECRET_ACCESS_KEY' => 'test-secret', 'AWS_SESSION_TOKEN' => 'test-token/+=', '_SESSION_EXPIRATION' => '2051222400'] as $key => $value) {
            check((string) $credentials[$key] === $value && $disk[$key] === $value, 'Startup did not update cache and disk: '.$key);
        }
        check(count(liveTimers($refresher)) === 1, 'Startup did not schedule exactly one renewal');
        check(!file_exists($path.'.tmp'), 'Startup left a temporary file');
    };
}

try {
    $case = $argv[1] ?? '';
    check(isset($cases[$case]), 'Unknown refresher case: '.$case);
    $refresher = fixture($directory, $sts);
    $cases[$case]();
    echo 'PASS refresher '.$case."\n";
} finally {
    foreach (EventLoop::getIdentifiers() as $id) { EventLoop::cancel($id); }
    $files = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($directory, FilesystemIterator::SKIP_DOTS), RecursiveIteratorIterator::CHILD_FIRST);
    foreach ($files as $file) {
        if ($file->isDir()) { rmdir($file->getPathname()); }
        else { unlink($file->getPathname()); }
    }
    rmdir($directory);
}
