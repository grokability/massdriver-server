<?php

namespace Massdriver;

use Aws\Sqs\SqsClient;
use Aws\Credentials\Credentials;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class MassdriverQueue {
    public static ?self $singleton = null;

    public ?LoopInterface $loop = null;
    public ?Credentials $credentials = null;
    public ?SqsClient $sqs_client = null;
    protected int $iterations = 0;
    protected float $start_time = 0.0;
    protected bool $draining = false;
    protected bool $sqs_request_pending = false;
    protected \Closure $signal_handler;

    const int MAX_SQS_MESSAGE_COUNT = 10;

    public static function new_slots_available()
    {
        // this is weird and janky and will break if we move to more than one queue
        $mdq = self::$singleton;
        //spin up a SQS queue reader if we don't have one already!
        //crap, i need an *instance* to find out if we have an SQS Queue operation in progress or not.
        if($mdq->draining || $mdq->sqs_request_pending) {
            return;
        }
        $mdq->QueueReceiveLoop();
    }

    function __construct(
        public string $queue_name,
        public int $max_concurrency,
        public int $max_iterations,
        public int $max_duration,
        public string $command_template,
        public mixed $log_stream,
        public int $visibility_timeout = 30,
        public int $poll_time = 20,
    ) {
        if(static::$singleton) {
            throw new \Exception("Queue already initialized!!!\n");
        }
        //NOTE: THIS IS *SYNCHRONOUS*
        $this->loop = Loop::get();
        ReactGuzzleTaskQueue::install($this->loop);
        print("Selected 'Loop' type: ".get_class($this->loop)."\n");

        $this->sqs_client = new SqsClient([
            'version' => '2012-11-05',
            'http_handler' => new ReactHttpHandler($this->loop),
        ]);

        // I _was_ thinking about doing some kind of 'credentials adapter' here, because refreshing tokens
        // *might* block for a few seconds, sometimes. But I think we can just live with it.

        Task::boot($this->loop, $this->sqs_client, $this->queue_name, $this->max_concurrency, $this->log_stream);
        $this->signal_handler = function () {
            if($this->draining) {
                print "Second Interrupt Signal Detected, exiting *NOW*\n";
                exit(1);
            }
            print "Interrupt Signal Detected! Allowing tasks to finish. (Hit Ctrl+C again to force exit)";
            $this->draining = true;
        };
        $this->loop->addSignal(SIGINT,$this->signal_handler);
        $this->start_time = microtime(true);
        $this->QueueReceiveLoop();
        static::$singleton = $this;
    }

    function nothing_left_to_do() {
        return Task::slots_remaining() == $this->max_concurrency && !$this->sqs_request_pending;
    }

    function QueueReceiveLoop() {
        if($this->draining && $this->nothing_left_to_do()) {
            $this->stop();
        }
        if($this->draining) {
            return;
        }
        if(Task::slots_remaining() == 0) {
            return;
        }
        if($this->iterations >= $this->max_iterations || microtime(true)-$this->start_time > $this->max_duration) {
            print("Iterations or duration has elapsed, switching to 'draining'\n");
            $this->draining = true;
            if($this->nothing_left_to_do()) {
                print("Nothing left to do anyways, stopping.\n");
                $this->stop();
                return;
            }
        }
        if($this->sqs_request_pending) {
            print "Error! an SQS request was already pending - bailing out.\n";
            return;
        }
        $params = [
            'AttributeNames' => ['SentTimestamp'],
            'MaxNumberOfMessages' => min(static::MAX_SQS_MESSAGE_COUNT, Task::slots_remaining()),
            'MessageSystemAttributeNames' => ['All'],
            'QueueUrl' => $this->queue_name,
            'WaitTimeSeconds' => $this->poll_time,
            'VisibilityTimeout' => (int) $this->visibility_timeout,
        ];
        $this->iterations++;
        $this->sqs_request_pending = true;
        print("Sending SQS Message for: ".$params['MaxNumberOfMessages']."\n");
        $this->sqs_client->receiveMessageAsync($params)->then(function ($results) {
            print("SQS response received!\n");
            $this->sqs_request_pending = false;
            foreach( $results->get('Messages') ?? [] as $message) {
                $payload = json_decode($message['Body'], true, 8, JSON_THROW_ON_ERROR);
                print_r($payload);
                // The actual job object is base64+serialize()'d in 'command'
                $job = $payload['data']['command'];
                print("Command is: ".$job."\n");
                $replacements = [
                    '{TENANT}' => $message['Attributes']['MessageGroupId'],
                    '{B64PAYLOAD}' => base64_encode($job)
                ];
                try {
                    // Some PHP Serialization formats have NUL bytes in them, and escapeshellarg completely freaks out about those
                    $job_escaped = escapeshellarg($job);
                    $replacements['{PAYLOAD}'] = $job_escaped;
                } catch (\Exception $e) {
                    if(str_contains($this->command_template,'{PAYLOAD}')) {
                        throw new \RuntimeException("Could not shell-escape job payload, and {PAYLOAD} was requested in the command template");
                    }
                }
                $command_to_run = str_replace(array_keys($replacements), array_values($replacements), $this->command_template);
                print "COMMAND TO RUN IS:\n$command_to_run\n";

                $task = new Task($command_to_run,$message['ReceiptHandle'],$this->visibility_timeout);
            }

            $this->loop->futureTick(fn () => $this->QueueReceiveLoop());
        })->otherwise(function ($error) {
            print "ERROR RUNNING SQS QUERY!: $error\n";

            $this->loop->futureTick(fn () => $this->QueueReceiveLoop());
        });
    }

    function __invoke() {
        $this->loop->run();
        return [$this->iterations,microtime(true) - $this->start_time];
    }

    function stop() {
        print "Stopping loop!\n";
        $this->loop->removeSignal(SIGINT,$this->signal_handler);
        $this->loop->stop();
    }


}
