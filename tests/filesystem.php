<?php
declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

use Amp\File\Driver\BlockingFilesystemDriver;
use Amp\File\Driver\ParallelFilesystemDriver;
use Amp\Future;
use Amp\Parallel\Worker\ContextWorkerPool;
use Massdriver\FederatedClientCredentialsRefresher;
use Revolt\EventLoop;
use function Amp\File\filesystem;

function check(bool $condition, string $message): void
{
    if (!$condition) { throw new RuntimeException($message); }
}

$mode = $argv[1] ?? 'blocking';
$pool = null;
if ($mode === 'parallel') {
    $pool = new ContextWorkerPool(1);
    filesystem(new ParallelFilesystemDriver($pool));
} elseif ($mode === 'blocking') {
    filesystem(new BlockingFilesystemDriver());
} else {
    throw new InvalidArgumentException('Unknown filesystem test driver: '.$mode);
}

$directory = sys_get_temp_dir().'/massdriver-filesystem-'.bin2hex(random_bytes(8));
mkdir($directory.'/tenant', 0700, true);
$path = $directory.'/tenant/.env';
$original = "APP_NAME=example\nAWS_ACCESS_KEY_ID=old\n";
file_put_contents($path, $original);
chmod($path, 0600);
$before = stat($path);
$reflection = new ReflectionClass(FederatedClientCredentialsRefresher::class);
$refresher = $reflection->newInstanceWithoutConstructor();
$refresher->directory = $directory;

try {
    check(\Massdriver\readFileContents($path) === $original, 'Buffered read failed');
    if ($pool !== null) {
        check($pool->getWorkerCount() === 1, 'Read bypassed the selected worker pool');
    }
    \Massdriver\writeFileContents($directory.'/probe', str_repeat('x', 131072).'tail', $path);
    check(file_get_contents($directory.'/probe') === str_repeat('x', 131072).'tail', 'Whole-file write failed');

    check(\Massdriver\readFileContents($directory.'/created') === '', 'Missing-file creation failed');
    $refresher->write_one_credential('tenant', Future::complete([
        'AWS_ACCESS_KEY_ID' => 'new-key', 'AWS_SESSION_TOKEN' => 'new-token',
    ]), Future::complete($original));
    $updated = file_get_contents($path);
    check(\Dotenv\Dotenv::parse($updated)['AWS_ACCESS_KEY_ID'] === 'new-key', 'Replacement failed');
    clearstatcache(true, $path);
    $after = stat($path);
    foreach (['uid', 'gid', 'mode'] as $field) {
        check($before[$field] === $after[$field], 'Metadata changed: '.$field);
    }

    // Rendering failure must preserve the original and leave no temporary file.
    $badValue = new class {
        public function __toString(): string { throw new TypeError('simulated render error'); }
    };
    $failed = false;
    try {
        $refresher->write_one_credential('tenant', Future::complete([
            'AWS_ACCESS_KEY_ID' => $badValue,
        ]), Future::complete($updated));
    } catch (TypeError $error) {
        check($error->getMessage() === 'simulated render error', 'Unexpected failure');
        $failed = true;
    }
    check($failed, 'Rendering failure was swallowed');
    check(file_get_contents($path) === $updated, 'Failed write replaced the original');
    check(!file_exists($path.'.tmp'), 'Temporary file survived failure cleanup');
    echo "PASS filesystem $mode\n";
} finally {
    $pool?->shutdown();
    filesystem(new BlockingFilesystemDriver());
    EventLoop::run();
    $files = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($directory, FilesystemIterator::SKIP_DOTS), RecursiveIteratorIterator::CHILD_FIRST);
    foreach ($files as $file) {
        if ($file->isDir()) { rmdir($file->getPathname()); }
        else { unlink($file->getPathname()); }
    }
    rmdir($directory);
}
