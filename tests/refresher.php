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
        check($name === 'getFederationTokenAsync', 'Unexpected STS operation');
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
    (new ReflectionProperty(Refresher::class, 'credentials_array'))->setValue(null, $credentials);
}

function liveTimers(Refresher $refresher): array
{
    return array_values(array_intersect(
        array_filter(state($refresher, 'refresh_credentials_timers')),
        EventLoop::getIdentifiers(),
    ));
}

// Construct without the real STS client so no credential discovery or network
// access is possible. All loading, scheduling, mapping and writing uses production methods.
function fixture(string $directory, FakeSts $sts): Refresher
{
    $reflection = new ReflectionClass(Refresher::class);
    $refresher = $reflection->newInstanceWithoutConstructor();
    $refresher->directory = $directory;
    $refresher->inline_role = '{"Version":"2012-10-17","Statement":[]}';
    $refresher->arns = ['arn:aws:iam::123456789012:policy/test'];
    $reflection->getProperty('sts_client')->setValue($refresher, $sts);
    $signal = EventLoop::onSignal(SIGHUP, static function (): void {});
    EventLoop::unreference($signal);
    $reflection->getProperty('signal')->setValue($refresher, $signal);
    return $refresher;
}

$directory = sys_get_temp_dir().'/massdriver-tests-'.bin2hex(random_bytes(8));
mkdir($directory.'/tenant', 0700, true);
$path = $directory.'/tenant/.env';
$original = "# keep this comment\n\nAPP_NAME=example\nAWS_ACCESS_KEY_ID=old-key\n_SESSION_EXPIRATION=2208988800\n";
file_put_contents($path, $original);
chmod($path, 0600);
$sts = new FakeSts();
$refresher = null;
$cases = [
    'mapping' => static function () use (&$refresher, $sts): void {
        $credentials = $refresher->refresh_one_credential('tenant')->await();
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
        $credentials = $refresher->refresh_one_credential('tenant');
        $write = $refresher->write_one_credential('tenant', $credentials, Future::complete($original));
        check($write->isComplete(), 'Successful write left its Future pending');
        $write->await();
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
            $refresher->write_one_credential('tenant', Future::error($failure), Future::complete($original))->await();
            throw new RuntimeException('Failed credentials unexpectedly succeeded');
        } catch (Throwable $error) { check($error === $failure, 'Original failure was lost'); }
        EventLoop::run(); // Detect delayed close callbacks overwriting the file.
        check(file_get_contents($path) === $original, 'Failed renewal changed the original file');
        check(!file_exists($path.'.tmp'), 'Failed renewal left a temporary file');
    },
    'write-error-cleanup' => static function () use (&$refresher, $path, $original): void {
        $failure = new TypeError('simulated credential transformation error');
        try {
            $refresher->write_one_credential('tenant', Future::error($failure), Future::complete($original))->await();
            throw new RuntimeException('Error unexpectedly succeeded');
        } catch (Throwable $error) { check($error === $failure, 'Original Error was lost'); }
        check(file_get_contents($path) === $original, 'Error changed the original file');
        check(!file_exists($path.'.tmp'), 'TypeError bypassed temporary-file cleanup');
    },
    'multiline' => static function () use (&$refresher, $path): void {
        $contents = "NOTES=\"first line\nAWS_ACCESS_KEY_ID=example-in-notes\nlast line\"\nAWS_ACCESS_KEY_ID=old\n";
        $before = Dotenv::parse($contents);
        file_put_contents($path, $contents);
        $refresher->write_one_credential('tenant', Future::complete(['AWS_ACCESS_KEY_ID' => 'new']), Future::complete($contents))->await();
        $after = Dotenv::parse(file_get_contents($path));
        check($after['NOTES'] === $before['NOTES'] && $after['AWS_ACCESS_KEY_ID'] === 'new', 'Multiline content was mistaken for a credential assignment');
    },
    'load' => static function () use (&$refresher, $sts): void {
        $refresher->load_credentials_from_disk()->await();
        check(state($refresher, 'credentials_array')['tenant']['AWS_ACCESS_KEY_ID'] === 'old-key', 'Tenant credentials were not loaded');
        check($sts->calls === [], 'Unexpired credentials were unnecessarily renewed');
        check(count(liveTimers($refresher)) === 1, 'Tenant renewal was not scheduled');
        $refresher->graceful_shutdown();
        check(liveTimers($refresher) === [], 'Shutdown left renewal timers');
        EventLoop::run();
    },
    'load-error' => static function () use (&$refresher, $path): void {
        file_put_contents($path, 'BROKEN="unterminated');
        $failed = false;
        try { $refresher->load_credentials_from_disk()->await(); }
        catch (Throwable) { $failed = true; }
        check($failed, 'Malformed tenant file was silently treated as a successful reload');
    },
    'backoff' => static function () use (&$refresher, $sts): void {
        seed($refresher, 'tenant', 0);
        $sts->failure = new RuntimeException('simulated STS outage');
        $refresher->single_credential_refresh_loop('tenant');
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
            try { $refresher->single_credential_refresh_loop($tenant); }
            catch (Throwable $error) { throw new RuntimeException('One tenant exhausted another tenant\'s retry budget', 0, $error); }
        }
    },
    'reschedule' => static function () use (&$refresher, $path): void {
        seed($refresher, 'tenant', microtime(true) + Refresher::MAX_EXPIRATION_THRESHOLD + 0.1);
        $refresher->single_credential_refresh_loop('tenant');
        // Another actor refreshes credentials before our earlier timer fires.
        file_put_contents($path, '_SESSION_EXPIRATION='.(time() + 7200));
        $refresher->load_credentials_from_disk()->await();
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
        check(liveTimers($refresher) === [], 'In-flight renewal scheduled another timer after shutdown');
    },
];

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
