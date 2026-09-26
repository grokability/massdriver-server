<?php

namespace Massdriver;

use Aws\Sqs\SqsClient;
use Massdriver\ReactAws\ReactAws;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;
use React\Promise\PromiseInterface;
use React\Stream\WritableResourceStream;

class Task
{
    protected static int $max_processes = 0;
    protected static LoopInterface $loop;
    protected static ReactAws $sqs_client;
    protected static string $queue_url;
    protected static mixed $error_stream;
    protected static MassdriverQueue $queue;
    protected static array $processes = [];

    protected mixed $stdout_handle;
    protected mixed $stderr_handle;
    protected string $stdout = '';
    protected string $stderr = '';
    protected mixed $process_handle;
    protected ?int $status = null;
    protected float $start_time;
    protected ?TimerInterface $timer = null;

    const int MAX_RETRIES = 5;

    public static function boot(
        LoopInterface $loop,
        ReactAws $sqs_client,
        string $queue_url,
        int $max_processes,
        MassdriverQueue $queue,
    )
    {
        static::$loop = $loop;
        static::$sqs_client = $sqs_client;
        static::$queue_url = $queue_url;
        static::$max_processes = $max_processes;
        static::$queue = $queue;
        static::$error_stream = new WritableResourceStream(STDERR, static::$loop);
        static::$loop->addSignal(
            SIGCHLD,  fn () => static::check_all_tasks_for_termination()  //[static::class,'all'],
        ); //TODO - do we need to removeSignal() for this at some point?
    }

    public static function slots_remaining()
    {
        return static::$max_processes - count(static::$processes);
    }

    public static function delete_process(int $index)
    {
        // This has to be declared in its own method because 'unset' is a language construct, and not a function
        unset(static::$processes[$index]);
    }

    public static function check_all_tasks_for_termination()
    {
        $slots = 0;
        foreach (static::$processes as $id => $process) {
            if($process->check_for_termination()) {
                $process->do_termination(fn () => static::delete_process($id));
                $slots++;
            }
        }
        print "Count of active processes? ".count(static::$processes)."\n";
        if($slots) {
            static::$queue->there_are_more_slots_available($slots);
        } else {
            print "No more slots, not signalling for more slots.\n";
        }
    }

    public function __construct(
        public string|array $command_to_run,
        public $receipt_handle,
        public int $visibility_window,
        public bool $delete_after_failure = false
    ) {
        if(!static::$loop || !static::$sqs_client) {
            throw new \Exception("The Task class was not booted, exiting");
        }
        $descriptor_spec = [
            0 => ['file', '/dev/null', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];
        $pipes = [];
        $this->process_handle = proc_open($command_to_run, $descriptor_spec, $pipes);
        if($this->process_handle === false) {
            throw new \Exception("Failed to start task: ".var_dump($command_to_run));
        }
        static::$processes[] = $this;

        $this->stdout_handle = $pipes[1];
        $this->stderr_handle = $pipes[2];

        $this->add_stream_handlers("stdout");
        $this->add_stream_handlers("stderr");

        $this->extend_visibility_window();
        $this->start_time = microtime(true);
    }

    public function add_stream_handlers(string $stream_name)
    {
        // Unfortunately, we *have* to use this low-level API, instead
        // of the far nicer ReadableResourceStream, because that one can error
        // out if the command you're running quickly closes stdout or stderr
        static::$loop->addReadStream($this->{$stream_name.'_handle'},
            function ($stream) use ($stream_name) {
                if(!$this->{$stream_name}) {
                    $is_resource = is_resource($stream);
                    $resource_type = $is_resource ? get_resource_type($stream) : gettype($stream);
                    if($is_resource && $resource_type === "stream") {
                        $this->{$stream_name} .= stream_get_contents($stream);
                    } else {
                        print "Stream $stream_name ".($is_resource ? "is": "is NOT")." a valid resource! It's a: ".($resource_type ?: "NULL resource")."\n";
                    }
                } else {
                    //don't seek (or try to get the contents of) busted streams
                    if(is_resource($this->{$stream_name.'_handle'}) && get_resource_type($stream) === "stream") {
                        if(fseek($this->{$stream_name.'_handle'},0,SEEK_END) !== 0) {
                            // We were unable to 'seek' for whatever reason, so just read the stream and throw it away
                            stream_get_contents($stream);
                        }
                    }
                }

            }
        );
    }

    public function change_visibility_window(int $new_window): PromiseInterface
    {
        return static::$sqs_client->changeMessageVisibilityAsync([
            'QueueUrl' => static::$queue_url,
            'ReceiptHandle' => $this->receipt_handle,
            'VisibilityTimeout' => $new_window,
        ]);
    }

    public function extend_visibility_window()
    {
        if(is_null($this->timer)) {
            print "Initial boot of timer\n";
            $this->timer = static::$loop->addTimer($this->visibility_window/2,fn() => $this->extend_visibility_window());
        } elseif(!$this->check_for_termination()) {
            $now = microtime(true);
            if($now - $this->start_time > $this->visibility_window/2) {
                $new_visibility_window = $this->visibility_window * 2;
                $this->change_visibility_window($new_visibility_window)->then(function () use ($new_visibility_window) {
                    $this->visibility_window = $new_visibility_window;
                    $this->timer = static::$loop->addTimer($this->visibility_window/2,fn() => $this->extend_visibility_window());
                })->otherwise(function ($reason) {
                    print "Error changing visibility window for handle: ".$this->receipt_handle.": $reason\n";
                    static::$loop->futureTick(fn () => $this->extend_visibility_window());
                });
            }
        }
    }

    public function check_for_termination() : bool
    {
        if(!$this->process_handle) {
            print "Process with status code ".$this->status." is already terminating; no need to force that again.\n";
            return false;
        }
        $status = proc_get_status($this->process_handle);

        if (!$status['running']) {
            $this->status = $status['exitcode'];
            return true;
        }

        return false;
    }

    public function deleteQueuedMessage()
    {
        static $retries = 0;
        if($this->receipt_handle) {
            static::$sqs_client->deleteMessageAsync([
                'QueueUrl' => self::$queue_url,
                'ReceiptHandle' => $this->receipt_handle,
            ])->then(function ($results) {
                print("Deletion success for " . $this->receipt_handle . "\n");
                $this->receipt_handle = null; // null out the handle so we don't accidentally try to delete it again
            })->otherwise(function ($reason) use (&$retries) {
                print "DeleteMessage call failed! Reason: $reason\n";
                // I don't know what to do now? I *guess* just 'try again'? probably needs to a limit in there somewhere though...
                if ($retries++ < self::MAX_RETRIES) {
                    $timer_duration = 5 * $retries;
                    print "Current iterations: $retries, seconds to wait: $timer_duration seconds\n";

                    static::$loop->addTimer($timer_duration, fn() => $this->do_termination(fn () => null));
                } else {
                    print "$retries iterations. Unable to delete this message, it's likely to show up again :/\n";
                    $this->process_handle = null;
                }
            });
        }
    }

    public function do_termination(\Closure $callback)
    {
        if(is_null($this->status)) {
            print "ERROR - cannot terminate unterminated process!\n";
            return;
        }
        static::$loop->cancelTimer($this->timer);

        if($this->process_handle) {
            //$this->handle *might* already be null from a previous, failed invocation of this routine
            // so we can't just bail on the whole loop; we need to keep going.
            proc_close($this->process_handle);
            $this->process_handle = null; //in case an additional termination message happens to fire while this one is being handled.
        }

        if($this->stdout_handle) {
            static::$loop->removeReadStream($this->stdout_handle);
            $this->stdout_handle = null;
        }
        if($this->stderr_handle) {
            static::$loop->removeReadStream($this->stderr_handle);
            $this->stderr_handle = null;
        }

        if($this->status === 0) {
            $this->deleteQueuedMessage();

            $callback(); //this deletes the process from the list; even though it may still be re-running its SQS Deletion
            // We don't signal the MassdriverQueue yet that there are slots available here.
            // We do it after the end of the process-poll in the static method check_all_tasks_for_termination().
            // that way, we make sure to have Massdriver fire off an SQS request for as many slots as we *truly*
            // have available.
        } else {
            if($this->delete_after_failure) {
                // in cron-mode, we do *NOT* retry messages that have non-zero status codes
                $this->deleteQueuedMessage();
                $callback();
            } else {
                //throw it right back onto the queue
                $this->change_visibility_window(0)->then(
                    fn () => print "Visibility Window set to 0\n"
                )->otherwise(
                    fn () => print "Could not change visibility window for failed task - whatevs.\n"
                );
                // and don't bother with retries - if that fails, let the normal visibility window handle it
            }
            // write things out to STDERR for later debugging
            static::$error_stream->write('Error running process: "'.(is_array($this->command_to_run) ? implode(" ".$this->command_to_run, true) : $this->command_to_run).'" errno: '.$this->status."\n");
            static::$error_stream->write("STDOUT:\n".$this->stdout."\n");
            static::$error_stream->write("STDERR:\n".$this->stderr."\n");
        }
    }
}
