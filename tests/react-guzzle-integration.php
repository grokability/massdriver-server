<?php
declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';

use Aws\Sqs\SqsClient;
use GuzzleHttp\Promise\CancellationException;
use GuzzleHttp\Promise\Promise;
use GuzzleHttp\Promise\Utils;
use GuzzleHttp\Psr7\Request;
use Massdriver\ReactGuzzleTaskQueue;
use Massdriver\ReactHttpHandler;
use React\EventLoop\StreamSelectLoop;
use React\Http\HttpServer;
use React\Http\Message\Response;
use React\Promise\Deferred;
use React\Socket\SocketServer;

function check(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

$loop = new StreamSelectLoop();
$order = [];
Utils::queue()->add(static function () use (&$order) { $order[] = 'before installation'; });
$queue = ReactGuzzleTaskQueue::install($loop);
check(ReactGuzzleTaskQueue::install($loop) === $queue, 'Installation was not idempotent');
$queue->add(static function () use (&$order, $queue) {
    $order[] = 'first';
    $queue->add(static function () use (&$order) { $order[] = 'nested'; });
});
$queue->add(static function () use (&$order) { $order[] = 'second'; });
check($order === [], 'Callbacks ran inline');
$loop->run();
check($order === ['before installation', 'first', 'second', 'nested'], 'Queue lost FIFO order or existing work');
check($queue->isEmpty(), 'Queue did not drain');
$loop->run(); // Must return immediately: no idle polling timer keeps the loop alive.
echo "PASS queue ordering, idempotent installation, and idle exit\n";

try {
    ReactGuzzleTaskQueue::install(new StreamSelectLoop());
    throw new RuntimeException('Accepted a different loop');
} catch (LogicException $expected) {
}

// A raw task throwing must not strand the remaining work if the caller resumes.
$ran = false;
$queue->add(static function () { throw new RuntimeException('expected'); });
$queue->add(static function () use (&$ran) { $ran = true; });
try {
    $loop->run();
} catch (RuntimeException $error) {
    check($error->getMessage() === 'expected', 'Unexpected queue exception');
}
$loop->run();
check($ran, 'Exception stranded queued work');

$recovered = null;
$promise = new Promise();
$promise->then(static fn () => throw new RuntimeException('handler failure'))
    ->otherwise(static fn () => 'recovered')
    ->then(static function ($value) use (&$recovered) { $recovered = $value; });
$promise->resolve('start');
$loop->run();
check($recovered === 'recovered', 'Promise rejection recovery stalled');
echo "PASS queue exceptions and Guzzle promise chains\n";

// Only loopback HTTP and dummy AWS keys are used; no AWS requests or credentials.
$requests = [];
$attempts = 0;
$slowArrived = false;
$cancelOnArrival = null;
$server = new HttpServer($loop, static function ($request) use (&$requests, &$attempts, &$slowArrived, &$cancelOnArrival) {
    $requests[] = [$request->getUri()->getPath(), microtime(true), $request->getHeaderLine('Authorization')];
    $headers = ['Content-Type' => 'application/x-amz-json-1.0', 'Connection' => 'close'];
    if ($request->getUri()->getPath() === '/slow') {
        $slowArrived = true;
        if ($cancelOnArrival !== null) {
            $cancelOnArrival->cancel();
        }
        return (new Deferred())->promise();
    }
    if ($request->getHeaderLine('X-Amz-Target') !== '') {
        if (++$attempts === 1) {
            return new Response(500, $headers, '{"__type":"InternalError","message":"try again"}');
        }
        return new Response(200, $headers, '{"Messages":[]}');
    }
    return new Response(200, $headers, '{}');
});
$socket = new SocketServer('127.0.0.1:0', [], $loop);
$connections = new SplObjectStorage();
$socket->on('connection', static function ($connection) use ($connections) {
    $connections->attach($connection);
    $connection->on('close', static function () use ($connections, $connection) {
        $connections->detach($connection);
    });
});
$server->listen($socket);
$url = str_replace('tcp://', 'http://', $socket->getAddress());
$handler = new ReactHttpHandler($loop);

function runRequest($promise): array
{
    global $loop;
    $outcome = [];
    $expired = false;
    $watchdog = $loop->addTimer(5, static function () use ($loop, &$expired) {
        $expired = true;
        $loop->stop();
    });
    $promise->then(
        static function ($value) use (&$outcome, $loop) { $outcome = ['value' => $value]; $loop->stop(); },
        static function ($reason) use (&$outcome, $loop) { $outcome = ['reason' => $reason]; $loop->stop(); },
    );
    $loop->run();
    $loop->cancelTimer($watchdog);
    check(!$expired && $outcome !== [], 'Request stalled without a pump');
    return $outcome;
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
    echo "PASS SQS signing, real React Browser HTTP, response parsing, and retry\n";

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
    $outcome = runRequest($handler(new Request('GET', $url . '/closed')));
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
$loop->run();
check($queue->isEmpty(), 'Work remained after cleanup');
echo "PASS cleanup and natural loop exit without a polling timer\n";
