<?php

namespace Massdriver\AmpAws;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Aws\AwsClient;
use Aws\Result;
use GuzzleHttp\Promise\Utils as GuzzleUtils;

/** AWS method names and Result objects are retained; async calls return Amp Futures. */
class AmpAws
{
    protected AwsClient $client;

    public function __construct(string $client_type, array $options = [])
    {
        GuzzleUtils::queue(new AmpGuzzleTaskQueue());
        $options['http_handler'] ??= new AmpHttpHandler();
        $clientname = 'Aws\\'.$client_type.'\\'.$client_type.'Client';
        $this->client = new $clientname($options);
    }

    public function __call(string $name, array $arguments): Result
    {
        //last argument *may* be a Cancellation
        $cancellation = null;
        if(end($arguments) instanceof Cancellation) {
            $cancellation = array_pop($arguments); //also *removes* the cancellation so AWS doesn't see it
        }
        $deferred = new DeferredFuture();
        $promise = $this->client->$name(...$arguments)->then(
            $deferred->complete(...)
        )->otherwise(
            static function ($reason) use ($deferred): void {
                $deferred->error($reason instanceof \Throwable ? $reason : new \RuntimeException('AWS transport failure'));
        });

        try {
            $subscription = $cancellation?->subscribe(static fn () => $promise->cancel());
            return $deferred->getFuture()->await($cancellation);
        } finally {
            if ($subscription !== null) {
                $cancellation->unsubscribe($subscription);
            }
        }
    }
}
