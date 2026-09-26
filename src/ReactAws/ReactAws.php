<?php

namespace Massdriver\ReactAws;

use Aws\AwsClient;
use Aws\Sqs\SqsClient;
use Aws\Sts\StsClient;
use GuzzleHttp\Promise\Utils;
use React\EventLoop\LoopInterface;
use React\Promise\PromiseInterface;
use function React\Promise\resolve;

class ReactAws {

    protected static bool $guzzle_task_queue_replaced = false;

    protected AwsClient $client;
    public function __construct(
        protected LoopInterface $loop,
        string $client_type,
        array $options=[]
    ) {
        $options['http_client'] = new ReactHttpHandler($this->loop);
        $clientname = 'Aws\\'.$client_type.'\\'.$client_type.'Client';
        $this->client = match ($client_type) {
            'Sqs' => new SqsClient($options), // we do these two explicitly just for IDE niceness
            'Sts' => new StsClient($options),
            default => new $clientname($options),
        };
        if(!static::$guzzle_task_queue_replaced) {
            $guzzle_queue = new ReactGuzzleTaskQueue($this->loop);
            Utils::queue($guzzle_queue);
            static::$guzzle_task_queue_replaced = true;
        }

    }

    public function __call($name, $arguments): PromiseInterface {
        $promise = resolve($this->client->$name(...$arguments));
        return $promise;
    }
}