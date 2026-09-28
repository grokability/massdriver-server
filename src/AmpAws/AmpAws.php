<?php

namespace Massdriver\AmpAws;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Aws\AwsClient;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Promise\Utils;
use function Amp\async;

/** AWS method names and Result objects are retained; async calls return Amp Futures. */
class AmpAws
{
    protected AwsClient $client;

    public function __construct(string $client_type, array $options = [])
    {
        Utils::queue(new AmpGuzzleTaskQueue());
        $options['http_handler'] ??= new AmpHttpHandler();
        $clientname = 'Aws\\'.$client_type.'\\'.$client_type.'Client';
        $this->client = new $clientname($options);
    }

    public function __call(string $name, array $arguments): Future
    {
        return async(static function () use ($promise, $cancellation) {
            $deferred = new DeferredFuture();
            $promise->then($deferred->complete(...), static function ($reason) use ($deferred): void {
                $deferred->error($reason instanceof \Throwable ? $reason : new \RuntimeException('AWS transport failure'));
            });
            $subscription = $cancellation?->subscribe(static fn () => $promise->cancel());
            try {
                return $deferred->getFuture()->await();
            } finally {
                if ($subscription !== null) {
                    $cancellation->unsubscribe($subscription);
                }
            }
        });self::awaitPromise($this->client->$name(...$arguments));
    }
}
