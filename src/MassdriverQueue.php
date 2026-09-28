<?php

namespace Massdriver;

use Aws\Credentials\Credentials;
use Massdriver\AmpAws\AmpAws;
use Amp\Future;
use Amp\DeferredCancellation;
use Revolt\EventLoop;
use function Amp\async;

class MassdriverQueue {
    public ?Credentials $credentials = null;
    public AmpAws $sqs_client;
    protected int $iterations = 0;
    protected float $start_time = 0.0;
    protected bool $draining = false;
    protected ?Future $sqs_request_pending = null;
    protected string $signal_handler;
    protected ?DeferredCancellation $receive_cancellation = null;

    const int MAX_SQS_MESSAGE_COUNT = 10;

    function __construct(
        public string $queue_name,
        public int $max_concurrency,
        public int $max_iterations,
        public int $max_duration,
        public string $command_template,
        public string $cron_template,
        public int $visibility_timeout = 30,
        public int $poll_time = 20,
        ?AmpAws $sqs_client = null,
    ) {
        //NOTE: THIS IS *SYNCHRONOUS*

        $this->sqs_client = $sqs_client ?? new AmpAws('Sqs',[
            'version' => '2012-11-05',
        ]);

        // I _was_ thinking about doing some kind of 'credentials adapter' here, because refreshing tokens
        // *might* block for a few seconds, sometimes. But I think we can just live with it.

        Task::boot($this->sqs_client, $this->queue_name, $this->max_concurrency,$this);
        $this->signal_handler = EventLoop::onSignal(SIGINT, function () {
            if($this->draining) {
                print "Second Interrupt Signal Detected, exiting *NOW*\n";
                exit(1);
            }
            print "Interrupt Signal Detected! Allowing tasks to finish. (Hit Ctrl+C again to force exit)\n";
            $this->graceful_shutdown();
        });
        $this->start_time = microtime(true);
        $this->QueueReceiveLoop();
    }

    function graceful_shutdown() : void {
        $this->draining = true;
        $this->receive_cancellation?->cancel();
        if ($this->nothing_left_to_do()) {
            $this->stop();
        }
    }

    function nothing_left_to_do() {
        return Task::slots_remaining() == $this->max_concurrency && !$this->sqs_request_pending;
    }

    function there_are_more_slots_available(int $slots) {
        if ($slots != Task::slots_remaining()) {
            print "Task::class has let us know that there are $slots more slots available, but the real count is: ".Task::slots_remaining()."; running QueueReceiveLoop()\n";
        }
        $this->QueueReceiveLoop();
    }

    function QueueReceiveLoop() {
        if($this->draining && $this->nothing_left_to_do()) {
            print "Draining *and* nothing left to do; stopping.\n";
            $this->stop();
        }
        if($this->draining) {
            print "Just draining. That's fine? Still, not going to do SQS stuff.\n";
            return;
        }
        if(Task::slots_remaining() == 0) {
            print "No slots remaining. No need to talk to SQS right now.\n";
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
            // note - each terminating process *will* fire QueueReceiveLoop again,
            // until the *last* one terminates and fires - then the first check
            // will pass and the loop will stop.
            return;
        }
        if($this->sqs_request_pending) {
            print "Error! an SQS request was already pending - bailing out.\n";
            return;
        }
        $params = [
            'MaxNumberOfMessages' => min(static::MAX_SQS_MESSAGE_COUNT, Task::slots_remaining()),
            'MessageSystemAttributeNames' => ['All'],
            'QueueUrl' => $this->queue_name,
            'WaitTimeSeconds' => $this->poll_time,
            'VisibilityTimeout' => $this->visibility_timeout,
            'MessageAttributeNames' => ['All'],
        ];
        $this->iterations++;
        print("Asking SQS for: ".$params['MaxNumberOfMessages']." messages.\n");

        $this->receive_cancellation = new DeferredCancellation();
        $this->sqs_request_pending = async(function () use ($params): void {
            try {
                $results = $this->sqs_client->receiveMessageAsync($params, $this->receive_cancellation->getCancellation())->await();
                print("SQS response received! Count: ".count($results->get('Messages') ?? [])."\n");

                foreach( $results->get('Messages') ?? [] as $message) {
                    $cron_mode = false;
                    if(($message['MessageAttributes']['subsystem']['StringValue'] ?? '') == 'cron') {
                        print "CRON MODE DETECTED!";
                        $cron_mode = true;

                        //make sure you have a command template!
                        if(!$this->cron_template) {
                            throw new \RuntimeException("Cron template is not set, but cron mode was detected");
                        }
                    }
                    $payload = json_decode($message['Body'], true, 8, JSON_THROW_ON_ERROR);
                    print_r($message);
                    $job = '';
                    $cmd = '';
                    if($cron_mode) {
                        // TODO - maybe just have the payload _be_ the command-line message?
                        // not sure yet until I see if we have to add more parameters to this or something
                        $cmd = $payload['cmd'] ?? throw new \RuntimeException("Couldn't find 'cmd' in cron-mode message");
                    } else {
                        // The actual job object is base64+serialize()'d in 'command'
                        $job = $payload['data']['command'] ?? throw new \RuntimeException("Couldn't find command, weird JSON");
                    }
                    print("Command is: ".($job ?: $cmd)."\n");

                    $replacements = [
                        '{TENANT}' => $message['Attributes']['MessageGroupId'],
                        '{B64PAYLOAD}' => base64_encode($job),
                        '{COMMANDLINE}' => $cmd,
                    ];

                    if($cron_mode) {
                        $command_to_run = str_replace(array_keys($replacements), array_values($replacements), $this->cron_template);
                    } else {
                        try {
                            // Some PHP Serialization formats have NUL bytes in them, and escapeshellarg completely freaks out about those
                            $job_escaped = escapeshellarg($job);
                            $replacements['{PAYLOAD}'] = $job_escaped;
                        } catch (\Throwable $e) {
                            if (str_contains($this->command_template, '{PAYLOAD}')) {
                                throw new \RuntimeException("Could not shell-escape job payload, and {PAYLOAD} was requested in the command template");
                            }
                        }
                        $command_to_run = str_replace(array_keys($replacements), array_values($replacements), $this->command_template);
                        print "COMMAND TO RUN IS:\n$command_to_run\n";
                    }
                    // note - '$cron_mode' dictates whether we delete on a failed execution
                    $task = new Task($command_to_run,$message['ReceiptHandle'],$this->visibility_timeout,$cron_mode);
                }

            } catch (\Throwable $error) {
                if (!$this->draining) {
                    print "ERROR RUNNING SQS QUERY!: $error\n";
                }
            } finally {
                $this->sqs_request_pending = null;
                $this->receive_cancellation = null;
                EventLoop::queue(fn () => $this->QueueReceiveLoop());
            }
        });
    }

    function __invoke() {
        EventLoop::run();
        return [$this->iterations,microtime(true) - $this->start_time];
    }

    function stop() {
        print "Stopping loop!\n";
        EventLoop::cancel($this->signal_handler);
        // Let pending deletes, visibility updates, and retries finish naturally.
    }


}
