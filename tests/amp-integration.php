<?php
declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';

\Amp\File\filesystem(new \Amp\File\Driver\BlockingFilesystemDriver());

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

if (($argv[1] ?? 'transport') === 'transport') {
$order = [];
$queue = new AmpGuzzleTaskQueue();
Utils::queue($queue);
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
$caught = false;
try {
    EventLoop::run();
} catch (\Throwable $error) {
    while ($error->getPrevious() !== null) { $error = $error->getPrevious(); }
    check($error->getMessage() === 'expected', 'Unexpected queue exception');
    $caught = true;
}
check($caught, 'Raw queue exception was swallowed');
EventLoop::run();
check($ran, 'Exception stranded queued work');

$recovered = null;
$promise = new Promise();
$promise->then(static fn () => throw new RuntimeException('handler failure'))
    ->otherwise(static fn () => 'recovered')
    ->then(static function ($value) use (&$recovered) { $recovered = $value; });
$promise->resolve('start');
EventLoop::run();
check($recovered === 'recovered', 'Promise rejection recovery stalled');
for ($i = 0; $i < 20; $i++) {
    $queue->add(static fn () => null);
    EventLoop::run();
    check($queue->isEmpty(), 'Repeated queue use retained completed work');
}
echo "PASS queue exceptions and Guzzle promise chains\n";

// Only loopback HTTP and dummy AWS keys are used; no AWS requests or credentials.
$requests = [];
$attempts = 0;
$slowArrived = false;
$cancelOnArrival = null;
$sdkArrived = new DeferredFuture();
$sdkClosed = new DeferredFuture();
$sdkRetryAttempts = 0;
$socket = listen('127.0.0.1:0');
$connections = new SplObjectStorage();
async(function () use ($socket, $connections, &$requests, &$attempts, &$slowArrived, &$cancelOnArrival, &$sdkArrived, &$sdkClosed, &$sdkRetryAttempts): void {
    while ($connection = $socket->accept()) {
        $connections->attach($connection);
        async(function () use ($connection, $connections, &$requests, &$attempts, &$slowArrived, &$cancelOnArrival, &$sdkArrived, &$sdkClosed, &$sdkRetryAttempts): void {
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
                    $chunk = $connection->read();
                    if ($chunk === null) { return; }
                    $body .= $chunk;
                }
                preg_match('/^\S+ (\S+)/', $head, $path);
                preg_match('/Authorization: ([^\r\n]+)/i', $head, $auth);
                $requests[] = [$path[1], microtime(true), $auth[1] ?? ''];
                $requestPath = rtrim($path[1], '/');
                if (str_starts_with($requestPath, '/sdk-cancel-')) {
                    if ($requestPath === '/sdk-cancel-retry') {
                        $sdkRetryAttempts++;
                        if ($sdkRetryAttempts === 1) {
                            $body = '{"__type":"InternalError","message":"try again"}';
                            $connection->write("HTTP/1.1 500 Response\r\nContent-Type: application/x-amz-json-1.0\r\nConnection: close\r\nContent-Length: ".strlen($body)."\r\n\r\n".$body);
                            return;
                        }
                    }
                    if ($requestPath === '/sdk-cancel-body') {
                        $connection->write("HTTP/1.1 200 Response\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: 100\r\n\r\n{");
                    }
                    $sdkArrived->complete();
                    $chunk = $connection->read();
                    while ($chunk !== null) {
                        $chunk = $connection->read();
                    }
                    $sdkClosed->complete();
                    return;
                }
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
    check($wrapper->receiveMessage(['QueueUrl' => $url.'/queue'])['Messages'] === [], 'Amp AWS wrapper failed');

    $before = count($requests);
    $cancellation = new \Amp\DeferredCancellation();
    $cancellation->cancel();
    try {
        $wrapper->receiveMessage(['QueueUrl' => $url.'/queue'], $cancellation->getCancellation());
        throw new RuntimeException('Already-cancelled SDK request unexpectedly succeeded');
    } catch (\Amp\CancelledException $expected) {
    }
    check(count($requests) === $before, 'Already-cancelled SDK request was sent');

    $before = count($requests);
    $cancellation = new \Amp\DeferredCancellation();
    $pending = async(fn () => $wrapper->receiveMessage([
        'QueueUrl' => $url.'/queue', '@http' => ['delay' => 50],
    ], $cancellation->getCancellation()));
    EventLoop::delay(0.01, fn () => $cancellation->cancel());
    try {
        $pending->await(new TimeoutCancellation(2));
        throw new RuntimeException('SDK cancellation unexpectedly succeeded');
    } catch (\Amp\CancelledException|CancellationException $expected) {
    }
    // Allow the original send deadline to pass, so abandoning the wait cannot pass.
    \Amp\delay(0.1);
    check(count($requests) === $before, 'SDK cancellation did not reach delayed transport');
    echo "PASS cancellation through AWS SDK and Amp Future bridge\n";

    foreach (['headers', 'body', 'retry'] as $phase) {
        $sdkArrived = new DeferredFuture();
        $sdkClosed = new DeferredFuture();
        $cancellation = new \Amp\DeferredCancellation();
        $cancellableWrapper = new AmpAws('Sqs', [
            'region' => 'us-east-2', 'version' => '2012-11-05',
            'endpoint' => $url.'/sdk-cancel-'.$phase,
            'credentials' => ['key' => 'test-key', 'secret' => 'test-secret'],
            'retries' => 1,
        ]);
        $pending = async(fn () => $cancellableWrapper->receiveMessage([
            'QueueUrl' => $url.'/queue', '@http' => ['timeout' => 5],
        ], $cancellation->getCancellation()));
        $sdkArrived->getFuture()->await(new TimeoutCancellation(2));
        if ($phase === 'retry') {
            check($sdkRetryAttempts === 2, 'Cancellation test did not reach the retry attempt');
        }
        if ($phase === 'body') {
            // Let Amp consume the headers and start buffering the incomplete body.
            \Amp\delay(0.01);
        }
        $cancellation->cancel();
        try {
            $pending->await(new TimeoutCancellation(2));
            throw new RuntimeException('In-flight SDK cancellation unexpectedly succeeded');
        } catch (\Amp\CancelledException|CancellationException $expected) {
        }
        // Caller cancellation alone is insufficient: the server must observe EOF.
        $sdkClosed->getFuture()->await(new TimeoutCancellation(1));
    }
    echo "PASS SDK cancellation closes HTTP while awaiting headers, body, or a retry response\n";

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

exit(0);
}

// Real child processes with a fake SQS boundary exercise the daemon lifecycle.
class FakeAws extends AmpAws
{
    public array $calls = [];
    public array $batches = [];
    public bool $slow = false;
    public ?Closure $afterReceive = null;
    public bool $cancelled = false;
    public int $deletionFailures = 0;
    public function __construct() {}
    public array $visibilityCompletions = [];
    public ?DeferredFuture $extensionStarted = null;
    public ?DeferredFuture $releaseExtension = null;
    public function receiveMessage(array $params, ?\Amp\Cancellation $cancellation = null): \Aws\Result
    {
        $this->calls[] = ['receive', $params];
        // Even an empty real HTTP response yields while waiting for I/O.
        \Amp\delay(0.001, cancellation: $cancellation);
        if ($this->slow) {
            try {
                \Amp\delay(10, cancellation: $cancellation);
            } catch (\Amp\CancelledException $e) {
                $this->cancelled = true;
                throw $e;
            }
        }
        $batch = array_shift($this->batches) ?? [];
        if ($batch instanceof Throwable) {
            if ($this->afterReceive !== null) { EventLoop::queue($this->afterReceive); }
            throw $batch;
        }
        if ($this->afterReceive !== null) {
            EventLoop::queue($this->afterReceive);
        }
        return new \Aws\Result(['Messages' => $batch]);
    }
    public function __call(string $name, array $arguments): \Aws\Result
    {
        $this->calls[] = [$name, $arguments[0]];
        \Amp\delay(0.01);
        if ($name === 'deleteMessage' && $this->deletionFailures-- > 0) {
            throw new RuntimeException('simulated delete failure');
        }
        if ($name === 'changeMessageVisibility') {
            $visibility = $arguments[0]['VisibilityTimeout'];
            if ($visibility > 0) {
                $this->extensionStarted?->complete();
                $this->releaseExtension?->getFuture()->await();
            }
            $this->visibilityCompletions[] = $visibility;
        }
        return new \Aws\Result();
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
function daemon(FakeAws $aws, string $command, int $iterations = 1, int $visibility = 30, int $concurrency = 2): \Massdriver\Foreperson
{
    $queue = new \Massdriver\SharedQueue(
        queue_name: 'http://localhost/queue', max_concurrency: $concurrency,
        command_template: $command, cron_template: $command,
        visibility_timeout: $visibility, poll_time: 0, sqs_client: $aws,
    );
    $foreperson = new \Massdriver\Foreperson($iterations, 30);
    $foreperson->register($queue);
    // Stop after the requested fake responses, rather than waiting for the
    // supervisor's minute-long accounting interval. Real jobs must still drain.
    $aws->afterReceive = static function () use ($queue, $foreperson, $iterations): void {
        if ($queue->get_iterations_count() >= $iterations) {
            $foreperson->graceful_shutdown();
        }
    };
    return $foreperson;
}
function listener(): \Massdriver\EventLoopTask
{
    return new class extends \Massdriver\EventLoopTask {
        public int $calls = 0;
        public static function get_env_vars(): array { return []; }
        public function __invoke(): void {}
        public function get_iterations_count(): int { return 0; }
        public function graceful_shutdown(): void { $this->calls++; }
    };
}
function phpCommand(string $code): string
{
    return escapeshellarg(PHP_BINARY).' -r '.escapeshellarg($code);
}
$cases = [
    'role-credentials' => static function (): void {
        $environment = [];
        foreach (['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN', 'AWS_PROFILE', 'AWS_WEB_IDENTITY_TOKEN_FILE', 'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI', 'AWS_CONTAINER_CREDENTIALS_FULL_URI', 'AWS_EC2_METADATA_DISABLED'] as $name) {
            $environment[$name] = getenv($name);
            putenv($name);
        }
        $paths = [];
        $authorization = '';
        $completed = false;
        try {
            $handler = static function ($request, array $options) use (&$paths, &$authorization): \GuzzleHttp\Promise\PromiseInterface {
                $paths[] = $request->getUri()->getPath();
                $path = $request->getUri()->getPath();
                $body = match ($path) {
                    '/latest/api/token' => 'metadata-token',
                    '/latest/meta-data/iam/security-credentials/' => 'test-role',
                    '/latest/meta-data/iam/security-credentials/test-role' => json_encode([
                        'Code' => 'Success', 'AccessKeyId' => 'role-key',
                        'SecretAccessKey' => 'role-secret', 'Token' => 'role-token',
                        'Expiration' => gmdate('c', time() + 3600),
                    ]),
                    default => '{"Messages":[]}',
                };
                if ($path !== '/latest/api/token' && str_starts_with($path, '/latest/')) {
                    check($request->getHeaderLine('x-aws-ec2-metadata-token') === 'metadata-token', 'IMDSv2 token missing');
                }
                if (!str_starts_with($path, '/latest/')) {
                    $authorization = $request->getHeaderLine('Authorization');
                    check($request->getHeaderLine('X-Amz-Security-Token') === 'role-token', 'Role session token missing');
                }
                $promise = new Promise();
                async(static function () use ($promise, $body): void {
                    \Amp\delay(0.001);
                    $promise->resolve(new \GuzzleHttp\Psr7\Response(200, ['Content-Type' => 'application/x-amz-json-1.0'], $body));
                });
                return $promise;
            };
            $client = new AmpAws('Sqs', [
                'region' => 'us-east-2', 'version' => '2012-11-05',
                'endpoint' => 'http://127.0.0.1:9',
                'ec2_metadata_service_endpoint' => 'http://127.0.0.1:9',
                'use_aws_shared_config_files' => false,
                'http_handler' => $handler,
            ]);
            async(static function () use ($client, &$completed): void {
                $result = $client->receiveMessage(['QueueUrl' => 'http://127.0.0.1:9/queue']);
                check($result['Messages'] === [], 'Role-authenticated receive failed');
                $completed = true;
            });
            EventLoop::run();
            check($completed, 'Loop exited during role credential discovery');
            check($paths === ['/latest/api/token', '/latest/meta-data/iam/security-credentials/', '/latest/meta-data/iam/security-credentials/test-role', '/'], 'Role credential requests bypassed the Amp transport: ' . json_encode($paths));
            check(str_contains($authorization, 'Credential=role-key/'), 'Receive was not signed with role credentials');
            EventLoop::run(); // Credential discovery must not leave an idle keepalive.
        } finally {
            foreach ($environment as $name => $value) {
                putenv($value === false ? $name : $name . '=' . $value);
            }
        }
    },
    'startup-idle' => static function (): void {
        $foreperson = new \Massdriver\Foreperson(100, 30);
        $subsystems = [];
        for ($index = 0; $index < 2; $index++) {
            $subsystem = new class extends \Massdriver\EventLoopTask {
                public bool $started = false;
                public bool $queued = false;
                public static function get_env_vars(): array { return []; }
                public function get_iterations_count(): int { return 0; }
                public function graceful_shutdown(): void {}
                public function __invoke(): void
                {
                    $this->started = true;
                    EventLoop::queue(function (): void { $this->queued = true; });
                }
            };
            $foreperson->register($subsystem);
            $subsystems[] = $subsystem;
        }
        $foreperson();
        foreach ($subsystems as $subsystem) {
            check($subsystem->started && $subsystem->queued, 'Loop exited before subsystem startup work ran');
        }
        [, $duration] = $foreperson->get_final_statistics();
        check($duration < 1, 'Idle supervisor waited for its duration limit');
    },
    'deletion-retry' => static function (): void {
        $aws = new FakeAws();
        $aws->batches = [new RuntimeException('receive failure'), [message('a'), message('b')]];
        $aws->deletionFailures = 1;
        $daemon = daemon($aws, phpCommand('fwrite(STDOUT, str_repeat("x", 200000)); fwrite(STDERR, str_repeat("y", 200000));'), 2);
        $daemon();
        [$iterations] = $daemon->get_final_statistics();
        check($iterations === 2 && \Massdriver\Task::slots_remaining() === 2, 'Receive recovery or concurrency slots failed');
        $deletes = array_filter($aws->calls, fn ($c) => $c[0] === 'deleteMessage');
        check(count($deletes) === 3, 'Deletion or retry was abandoned during shutdown');
        echo "PASS receive recovery, output draining, concurrency slots, deletion retry, and graceful drain\n";
    },
    'malformed-batch' => static function (): void {
        $aws = new FakeAws();
        $invalid = message('invalid');
        $invalid['Body'] = '{invalid json';
        $aws->batches = [[$invalid, message('valid')]];
        daemon($aws, phpCommand('exit(0);'))();
        $deletes = array_values(array_filter($aws->calls, fn ($c) => $c[0] === 'deleteMessage'));
        check(count($deletes) === 1 && $deletes[0][1]['ReceiptHandle'] === 'valid', 'Malformed message prevented the rest of its batch from running');
    },
    'failed-jobs' => static function (): void {
        $aws = new FakeAws();
        $aws->batches = [[message('failed'), message('cron-failed', true)]];
        daemon($aws, phpCommand('fclose(STDOUT); fclose(STDERR); exit(7);'))();
        check(\Massdriver\Task::slots_remaining() === 2, 'Failed tasks leaked slots');
        check(count(array_filter($aws->calls, fn ($c) => $c[0] === 'changeMessageVisibility' && $c[1]['VisibilityTimeout'] === 0)) === 1, 'Failed job was not released');
        check(count(array_filter($aws->calls, fn ($c) => $c[0] === 'deleteMessage' && $c[1]['ReceiptHandle'] === 'cron-failed')) === 1, 'Failed cron was not deleted');
        echo "PASS failed jobs, cron failure deletion, and early pipe closure\n";
    },
    'visibility' => static function (): void {
        $aws = new FakeAws();
        $aws->batches = [[message('long')]];
        daemon($aws, phpCommand('usleep(650000);'), visibility: 1)();
        check(count(array_filter($aws->calls, fn ($c) => $c[0] === 'changeMessageVisibility' && $c[1]['VisibilityTimeout'] === 2)) === 1, 'Visibility was not extended');
        echo "PASS visibility extension and timer cleanup\n";
    },
    'shutdown' => static function (): void {
        $aws = new FakeAws();
        $aws->slow = true;
        $daemon = daemon($aws, phpCommand('exit(0);'));
        EventLoop::delay(0.02, fn () => $daemon->graceful_shutdown());
        $daemon();
        check($aws->cancelled, 'Shutdown did not cancel long polling');
        echo "PASS shutdown cancellation of pending receive\n";
    },
    'single-worker' => static function (): void {
        $aws = new FakeAws();
        $aws->batches = [[message('one')], [message('two')], [message('three')]];
        daemon($aws, phpCommand('exit(0);'), iterations: 3, concurrency: 1)();
        check(count(array_filter($aws->calls, fn ($c) => $c[0] === 'receive')) === 3,
            'Single worker stopped polling before all iterations completed');
        check(\Massdriver\Task::slots_remaining() === 1, 'Single worker slot was not released');
        echo "PASS single-worker slot release and continued polling\n";
    },
    'visibility-race' => static function (): void {
        // The child waits until an extension is in flight, then fails. Release the
        // extension only after join() observes exit, making the race deterministic.
        $marker = tempnam(sys_get_temp_dir(), 'massdriver-exit-');
        unlink($marker);
        try {
            $aws = new FakeAws();
            $aws->extensionStarted = new DeferredFuture();
            $aws->releaseExtension = new DeferredFuture();
            $aws->batches = [[message('visibility-race')]];
            $daemon = daemon($aws, phpCommand(
                '$deadline = microtime(true) + 10; while (!file_exists('.var_export($marker, true).')) { '
                .'if (microtime(true) > $deadline) { exit(99); } usleep(1000); } exit(7);'
            ), visibility: 1);
            async(function () use ($aws, $marker): void {
                $aws->extensionStarted->getFuture()->await();
                touch($marker);
                // Wait for Task's actual termination state, not a guessed process duration.
                $tasks = new ReflectionProperty(\Massdriver\Task::class, 'processes');
                $terminating = new ReflectionProperty(\Massdriver\Task::class, 'terminating');
                while (true) {
                    foreach ($tasks->getValue() as $task) {
                        if ($terminating->getValue($task)) {
                            $aws->releaseExtension->complete();
                            return;
                        }
                    }
                    \Amp\delay(0.001);
                }
            });
            $daemon();
            check($aws->visibilityCompletions === [2, 0], 'In-flight extension overwrote the failure reset');
        } finally {
            if (is_file($marker)) { unlink($marker); }
        }
        echo "PASS in-flight visibility extension finishes before failure reset\n";
    },
    'shutdown-listener' => static function (): void {
        $aws = new FakeAws();
        $daemon = daemon($aws, phpCommand('exit(0);'));
        $listener = listener();
        $daemon->register($listener);
        $daemon();
        check($listener->calls === 1, 'Registered component did not receive graceful shutdown');
        echo "PASS registered graceful-shutdown component\n";
    },
    'supervisor-accounting' => static function (): void {
        $aws = new FakeAws();
        $queue = new \Massdriver\SharedQueue(
            queue_name: 'http://localhost/queue', max_concurrency: 1,
            visibility_timeout: 30, poll_time: 0, sqs_client: $aws,
        );
        $foreperson = new class(1, 30) extends \Massdriver\Foreperson {
            const float ACCOUNTING_PERIOD = 0.01;
        };
        $listener = listener();
        $foreperson->register($queue);
        $foreperson->register($listener);
        $foreperson();
        [$iterations, $duration] = $foreperson->get_final_statistics();
        check($iterations >= 1 && $duration < 1, 'Supervisor did not stop polling at its accounting check');
        check($listener->calls === 1, 'Accounting shutdown missed or duplicated its listener');
    },
    'supervisor-signals' => static function (): void {
        $aws = new FakeAws();
        $aws->slow = true;
        $foreperson = daemon($aws, phpCommand('exit(0);'));
        $listener = new class extends \Massdriver\EventLoopTask {
            public int $reloads = 0;
            public int $shutdowns = 0;
            public static function get_env_vars(): array { return []; }
            public function __invoke(): void {}
            public function get_iterations_count(): int { return 0; }
            public function reload(): void { $this->reloads++; }
            public function graceful_shutdown(): void { $this->shutdowns++; }
        };
        $foreperson->register($listener);
        EventLoop::delay(0.01, static function (): void { posix_kill(getmypid(), SIGHUP); });
        EventLoop::delay(0.03, static function (): void { posix_kill(getmypid(), SIGINT); });
        $foreperson();
        check($listener->reloads === 1 && $listener->shutdowns === 1, 'Supervisor did not dispatch reload and shutdown signals');
        check($aws->cancelled, 'SIGINT did not cancel the pending receive');
    },
];
$case = $argv[1] ?? '';
if (!isset($cases[$case])) { throw new RuntimeException('Unknown integration case: '.$case); }
$watchdog = EventLoop::delay(20, static fn () => throw new RuntimeException('Daemon test stalled'));
EventLoop::unreference($watchdog);
try {
    $cases[$case]();
} finally {
    EventLoop::cancel($watchdog);
}
