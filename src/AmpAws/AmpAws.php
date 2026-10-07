<?php

namespace Massdriver\AmpAws;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Aws\AwsClient;
use Aws\Result;
use GuzzleHttp\Promise\Utils as GuzzleUtils;

/** Ordinary AWS method names await internally and return AWS Results. */
class AmpAws
{
    protected AwsClient $client;

    public function __construct(string $client_type, array $options = [])
    {
        GuzzleUtils::queue(new AmpGuzzleTaskQueue());
        $handler = new AmpHttpHandler($options);
        $clientname = 'Aws\\'.$client_type.'\\'.$client_type.'Client';
        $this->client = new $clientname($handler->get_options());
    }

    public function __call(string $name, array $arguments): Result
    {
        //last argument *may* be a Cancellation
        $cancellation = null;
        if(end($arguments) instanceof Cancellation) {
            $cancellation = array_pop($arguments); //also *removes* the cancellation so AWS doesn't see it
            $cancellation->throwIfRequested();
            $params_key = array_key_exists('args', $arguments) ? 'args' : 0;
            // Pass directly to HTTP: SDK promise adoption can break cancel() propagation.
            $arguments[$params_key]['@http'][AmpHttpHandler::CANCELLATION_OPTION] = $cancellation;
        }
        // The SDK's auth resolver calls wait(); resolve credentials through Amp first.
        AmpHttpHandler::credentials_future($this->client)->await($cancellation);
        $deferred = new DeferredFuture();
        $name .= "Async"; //switches to the Async method, but running synchronously
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
