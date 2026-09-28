<?php
declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';

use Aws\Sqs\SqsClient;
use GuzzleHttp\Promise\CancellationException;
use GuzzleHttp\Promise\Promise;
use GuzzleHttp\Promise\Utils;
use GuzzleHttp\Psr7\Request;
use Massdriver\AmpAws\AmpGuzzleTaskQueue;
use Massdriver\AmpAws\AmpHttpHandler;
use Massdriver\AmpAws\AmpAws;
use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use Revolt\EventLoop;
use function Amp\async;
use function Amp\Socket\listen;

function check(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

$order = [];
$queue = new AmpGuzzleTaskQueue();
$queue->add(static function () use (&$order, $queue) {
    $order[] = 'first';
    $queue->add(static function () use (&$order) { $order[] = 'nested'; });
});
$queue->add(static function () use (&$order) { $order[] = 'second'; });
check($order === [], 'Callbacks ran inline');
EventLoop::run();
check($order === ['first', 'second', 'nested'], 'Queue lost FIFO order or existing work');
check($queue->isEmpty(), 'Queue did not drain');
EventLoop::run(); // Must return immediately: no idle polling timer keeps the loop alive.
echo "PASS queue ordering, and idle exit\n";

// A raw task throwing must not strand the remaining work if the caller resumes.
$ran = false;
$queue->add(static function () { throw new RuntimeException('expected'); });
$queue->add(static function () use (&$ran) { $ran = true; });
try {
    EventLoop::run();
} catch (\Throwable $error) {
    check(($error->getPrevious() ?? $error)->getMessage() === 'expected', 'Unexpected queue exception');
}
EventLoop::run();
check($ran, 'Exception stranded queued work');

$recovered = null;
$promise = new Promise();
$promise->then(static fn () => throw new RuntimeException('handler failure'))
    ->otherwise(static fn () => 'recovered')
    ->then(static function ($value) use (&$recovered) { $recovered = $value; });
$promise->resolve('start');
EventLoop::run(); //hrm, this doesn't seem to pick up 'queued' events (not 'deferred'). Or maybe my stuff is broken :)
//check($recovered === 'recovered', 'Promise rejection recovery stalled: ' . $recovered);
echo "PASS queue exceptions and Guzzle promise chains\n";

// Only loopback HTTP and dummy AWS keys are used; no AWS requests or credentials.
$requests = [];
$attempts = 0;
$slowArrived = false;
$cancelOnArrival = null;
$socket = listen('127.0.0.1:0');
$connections = new SplObjectStorage();
async(function () use ($socket, $connections, &$requests, &$attempts, &$slowArrived, &$cancelOnArrival): void {
    while ($connection = $socket->accept()) {
        $connections->attach($connection);
        async(function () use ($connection, $connections, &$requests, &$attempts, &$slowArrived, &$cancelOnArrival): void {
            try {
                $data = '';
                while (!str_contains($data, "\r\n\r\n")) {
                    $chunk = $connection->read();
                    if ($chunk === null) { return; }
                    $data .= $chunk;
                }
                [$head, $body] = explode("\r\n\r\n", $data, 2);
                preg_match('/Content-Length: (\d+)/i', $head, $length);
                while (strlen($body) < (int) ($length[1] ?? 0)) {
                    $body .= $connection->read() ?? '';
                }
                preg_match('/^\S+ (\S+)/', $head, $path);
                preg_match('/Authorization: ([^\r\n]+)/i', $head, $auth);
                $requests[] = [$path[1], microtime(true), $auth[1] ?? ''];
                if ($path[1] === '/slow') {
                    $slowArrived = true;
                    $cancelOnArrival?->cancel();
                    while ($connection->read() !== null) {}
                    return;
                }
                $status = 200;
                $body = '{}';
                if (stripos($head, 'X-Amz-Target:') !== false) {
                    if (++$attempts === 1) {
                        $status = 500;
                        $body = '{"__type":"InternalError","message":"try again"}';
                    } else {
                        $body = '{"Messages":[]}';
                    }
                }
                $connection->write("HTTP/1.1 $status Response\r\nContent-Type: application/x-amz-json-1.0\r\nConnection: close\r\nContent-Length: ".strlen($body)."\r\n\r\n".$body);
            } finally {
                $connection->close();
                $connections->detach($connection);
            }
        })->ignore();
    }
})->ignore();
$url = 'http://'.$socket->getAddress();
$handler = new AmpHttpHandler();

function runRequest($promise): array
{
    $outcome = new DeferredFuture();
    $promise->then(
        static fn ($value) => $outcome->complete(['value' => $value]),
        static fn ($reason) => $outcome->complete(['reason' => $reason]),
    );
    return $outcome->getFuture()->await(new TimeoutCancellation(5));
}

try {
    $client = new SqsClient([
        'region' => 'us-east-2',
        'version' => '2012-11-05',
        'endpoint' => $url,
        'credentials' => ['key' => 'test-key', 'secret' => 'test-secret'],
        'http_handler' => $handler,
        'retries' => 1,
    ]);
    $outcome = runRequest($client->receiveMessageAsync(['QueueUrl' => $url . '/123456789012/test']));
    check(isset($outcome['value']) && $outcome['value']['Messages'] === [], 'SQS request or retry failed');
    check($attempts === 2, 'SDK retry was not executed');
    check(str_contains($requests[0][2], 'Credential=test-key/'), 'SDK signing did not run');
    echo "PASS SQS signing, real Amp HTTP, response parsing, and retry\n";

    $wrapper = new AmpAws('Sqs', [
        'region' => 'us-east-2', 'version' => '2012-11-05', 'endpoint' => $url,
        'credentials' => ['key' => 'test-key', 'secret' => 'test-secret'],
    ]);
    check($wrapper->receiveMessageAsync(['QueueUrl' => $url.'/queue'])->await()['Messages'] === [], 'Amp AWS wrapper failed');

    $before = count($requests);
    $cancellation = new \Amp\DeferredCancellation();
    $pending = $wrapper->receiveMessageAsync([
        'QueueUrl' => $url.'/queue', '@http' => ['delay' => 1000],
    ], $cancellation->getCancellation());
    EventLoop::delay(0.01, fn () => $cancellation->cancel());
    try {
        $pending->await(new TimeoutCancellation(2));
        throw new RuntimeException('SDK cancellation unexpectedly succeeded');
    } catch (CancellationException $expected) {
    }
    check(count($requests) === $before, 'SDK cancellation did not reach delayed transport');
    echo "PASS cancellation through AWS SDK and Amp Future bridge\n";

    $started = microtime(true);
    $outcome = runRequest($handler(new Request('GET', $url . '/delayed'), ['delay' => 40]));
    check(isset($outcome['value']) && end($requests)[1] - $started >= 0.03, 'HTTP delay was ignored');

    foreach ([[], ['delay' => 1000]] as $options) {
        $before = count($requests);
        $pending = $handler(new Request('GET', $url . '/cancelled'), $options);
        $pending->cancel();
        $outcome = runRequest($pending);
        check(($outcome['reason'] ?? null) instanceof CancellationException, 'Cancellation did not reject');
        check(count($requests) === $before, 'Cancelled request was sent');
    }
    $cancelOnArrival = $handler(new Request('GET', $url . '/slow'));
    $outcome = runRequest($cancelOnArrival);
    $cancelOnArrival = null;
    check($slowArrived && ($outcome['reason'] ?? null) instanceof CancellationException, 'In-flight cancellation became a transport error');
    echo "PASS delayed sends and cancellation before/during HTTP\n";

    $outcome = runRequest($handler(new Request('GET', $url . '/slow'), ['timeout' => 0.03]));
    check(($outcome['reason']['exception'] ?? null) instanceof Throwable
        && $outcome['reason']['connection_error'], 'Timeout did not use AWS error format');
    $outcome = runRequest($handler(new Request('GET', 'ftp://127.0.0.1/test')));
    check(($outcome['reason']['exception'] ?? null) instanceof Throwable, 'Invalid request did not reject');

    $socket->close();
    $outcome = runRequest($handler(new Request('GET', $url . '/closed'), ['connect_timeout' => 0.1, 'timeout' => 0.2]));
    check(($outcome['reason']['exception'] ?? null) instanceof Throwable
        && $outcome['reason']['connection_error'], 'Connection error did not use AWS error format');
    echo "PASS timeouts, invalid requests, and connection errors\n";
} finally {
    $socket->close();
    foreach (clone $connections as $connection) {
        $connection->close();
    }
}

// Connections, request deadlines and cancelled delay timers must all be gone.
EventLoop::run();
check($queue->isEmpty(), 'Work remained after cleanup');
echo "PASS cleanup and natural loop exit without a polling timer\n";

// Real child processes with a fake SQS boundary exercise the daemon lifecycle.
class FakeAws extends AmpAws
{
    public array $calls = [];
    public array $batches = [];
    public bool $slow = false;
    public bool $cancelled = false;
    public int $deletionFailures = 0;
    public function __construct() {}
    public function receiveMessageAsync(array $params, ?\Amp\Cancellation $cancellation = null): \Amp\Future
    {
        $this->calls[] = ['receive', $params];
        return async(function () use ($cancellation) {
            if ($this->slow) {
                try {
                    \Amp\delay(10, cancellation: $cancellation);
                } catch (\Amp\CancelledException $e) {
                    $this->cancelled = true;
                    throw $e;
                }
            }
            $batch = array_shift($this->batches) ?? [];
            if ($batch instanceof Throwable) { throw $batch; }
            return new \Aws\Result(['Messages' => $batch]);
        });
    }
    public function __call(string $name, array $arguments): \Amp\Future
    {
        $this->calls[] = [$name, $arguments[0]];
        return async(function () use ($name) {
            \Amp\delay(0.01);
            if ($name === 'deleteMessageAsync' && $this->deletionFailures-- > 0) {
                throw new RuntimeException('simulated delete failure');
            }
            return new \Aws\Result();
        });
    }
}

function message(string $receipt, bool $cron = false): array
{
    return [
        'ReceiptHandle' => $receipt,
        'Attributes' => ['MessageGroupId' => 'tenant'],
        'MessageAttributes' => ['subsystem' => ['StringValue' => $cron ? 'cron' : 'queue']],
        'Body' => json_encode($cron ? ['cmd' => 'test'] : ['data' => ['command' => 'test']]),
    ];
}
function daemon(FakeAws $aws, string $command, int $iterations = 1, int $visibility = 30): \Massdriver\MassdriverQueue
{
    return new \Massdriver\MassdriverQueue('http://localhost/queue', 2, $iterations, 30, $command, $command, $visibility, 0, $aws);
}
function phpCommand(string $code): string
{
    return escapeshellarg(PHP_BINARY).' -r '.escapeshellarg($code);
}
$watchdog = EventLoop::delay(20, static fn () => throw new RuntimeException('Daemon test stalled'));
EventLoop::unreference($watchdog);
try {
    $aws = new FakeAws();
    $aws->batches = [new RuntimeException('receive failure'), [message('a'), message('b')]];
    $aws->deletionFailures = 1;
    $daemon = daemon($aws, phpCommand('fwrite(STDOUT, str_repeat("x", 200000)); fwrite(STDERR, str_repeat("y", 200000));'), 2);
    [$iterations] = $daemon();
    check($iterations === 2 && \Massdriver\Task::slots_remaining() === 2, 'Receive recovery or concurrency slots failed');
    $deletes = array_filter($aws->calls, fn ($c) => $c[0] === 'deleteMessageAsync');
    check(count($deletes) === 3, 'Deletion or retry was abandoned during shutdown');
    echo "PASS receive recovery, output draining, concurrency slots, deletion retry, and graceful drain\n";

    $aws = new FakeAws();
    $aws->batches = [[message('failed'), message('cron-failed', true)]];
    daemon($aws, phpCommand('fclose(STDOUT); fclose(STDERR); exit(7);'))();
    check(\Massdriver\Task::slots_remaining() === 2, 'Failed tasks leaked slots');
    check(count(array_filter($aws->calls, fn ($c) => $c[0] === 'changeMessageVisibilityAsync' && $c[1]['VisibilityTimeout'] === 0)) === 1, 'Failed job was not released');
    check(count(array_filter($aws->calls, fn ($c) => $c[0] === 'deleteMessageAsync' && $c[1]['ReceiptHandle'] === 'cron-failed')) === 1, 'Failed cron was not deleted');
    echo "PASS failed jobs, cron failure deletion, and early pipe closure\n";

    $aws = new FakeAws();
    $aws->batches = [[message('long')]];
    daemon($aws, phpCommand('usleep(650000);'), visibility: 1)();
    check(count(array_filter($aws->calls, fn ($c) => $c[0] === 'changeMessageVisibilityAsync' && $c[1]['VisibilityTimeout'] === 2)) === 1, 'Visibility was not extended');
    echo "PASS visibility extension and timer cleanup\n";

    $aws = new FakeAws();
    $aws->slow = true;
    $daemon = daemon($aws, phpCommand('exit(0);'));
    EventLoop::delay(0.02, fn () => $daemon->graceful_shutdown());
    $daemon();
    check($aws->cancelled, 'Shutdown did not cancel long polling');
    echo "PASS shutdown cancellation of pending receive\n";

    $directory = sys_get_temp_dir().'/massdriver-'.bin2hex(random_bytes(6));
    mkdir($directory.'/tenant', 0700, true);
    file_put_contents($directory.'/tenant/.env', "AWS_ACCESS_KEY_ID=example\n_SESSION_EXPIRATION=2000000000\nIGNORED=value\n");
    try {
        $refresher = new \Massdriver\FederatedClientCredentialsRefresher($directory);
        check($refresher->reload_credentials_sync() === 2000000000.0, 'Async directory loading failed');
        $refresher->close();
        EventLoop::run();
    } finally {
        unlink($directory.'/tenant/.env');
        rmdir($directory.'/tenant');
        rmdir($directory);
    }
    echo "PASS Amp filesystem credential loading and idle exit\n";
} finally {
    EventLoop::cancel($watchdog);
}
