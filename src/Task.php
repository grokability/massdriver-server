<?php

namespace Massdriver;

use Amp\Future;
use Amp\Process\Process;
use Massdriver\AmpAws\AmpAws;
use Revolt\EventLoop;
use function Amp\async;
use function Amp\ByteStream\getStderr;
use function Amp\Future\await;

class Task
{
    protected static int $max_processes = 0;
    protected static AmpAws $sqs_client;
    protected static string $queue_url;
    protected static MassdriverQueue $queue;
    protected static array $processes = [];

    public string $id;
    protected string $stdout = '';
    protected string $stderr = '';
    protected Process $process_handle;
    protected ?int $status = null;
    protected float $start_time;
    protected ?string $visibility_extension_timer = null;
    protected bool $terminating = false;
    protected array $readers = [];
    protected ?Future $visibility_update = null;
    protected int $delete_retries = 0;

    const int MAX_RETRIES = 5;
    const int BUFFER_SIZE= 65536;

    public static function boot(AmpAws $sqs_client, string $queue_url, int $max_processes, MassdriverQueue $queue)
    {
        static::$sqs_client = $sqs_client;
        static::$queue_url = $queue_url;
        static::$max_processes = $max_processes;
        static::$queue = $queue;
    }

    public static function slots_remaining()
    {
        return static::$max_processes - count(static::$processes);
    }

    public function __construct(
        public string|array $command_to_run,
        public $receipt_handle,
        public int $visibility_window,
        public bool $delete_after_failure = false,
    ) {
        if (!isset(static::$sqs_client)) {
            throw new \LogicException('The Task class was not booted');
        }
        $this->process_handle = Process::start($command_to_run);
        $this->process_handle->getStdin()->close();
        $this->id = spl_object_id($this);
        static::$processes[$this->id] = $this;
        $this->start_time = microtime(true);
        $this->add_stream_handlers('stdout');
        $this->add_stream_handlers('stderr');
        $this->schedule_visibility_extension();
        $this->handle_task_exit();
    }

    public function handle_task_exit(): Future
    {
        return async(function () : void {
            $this->status = $this->process_handle->join();
            $this->terminating = true;
            if ($this->visibility_extension_timer !== null) {
                EventLoop::cancel($this->visibility_extension_timer);
                $this->visibility_extension_timer = null;
            }
            // Drain everything concurrently, including output buffered at exit.
            $futures = $this->readers;

            if ($this->status === 0 || $this->delete_after_failure) {
                $futures[] = $this->deleteQueuedMessage();
            } else {
                try {
                    if($this->visibility_update) {
                        // need to make sure the *old* one comes through before the *new* one fires off
                        $this->visibility_update->await();
                    }
                    $futures[] = $this->change_visibility_window(0);
                } catch(\Throwable $error) {
                    print "Could not reset visibility for failed task: $error\n";
                };
            }
            await($futures);
            // Release slots on every exit path, including failed jobs.

            if ($this->status !== 0) {
                $command = is_array($this->command_to_run) ? implode(' ', $this->command_to_run) : $this->command_to_run;
                getStderr()->write("Error running process: $command; errno: {$this->status}\nSTDOUT:\n{$this->stdout}\nSTDERR:\n{$this->stderr}\n");
            }
        })->catch(static function (\Throwable $error): void {
            getStderr()->write("Task completion failed: $error\n");
        })->finally(function () {
            unset(static::$processes[$this->id]);
            static::$queue->there_are_more_slots_available(1);
        });
    }

    public function add_stream_handlers(string $stream_name)
    {
        $stream = match ($stream_name) {
            'stdout' => $this->process_handle->getStdout(),
            'stderr' => $this->process_handle->getStderr(),
        };
        $this->readers[] = async(function () use ($stream, $stream_name): void {
            while (($chunk = $stream->read()) !== null) {
                // Retain bounded diagnostics while continuing to drain large output.
                $remaining = self::BUFFER_SIZE - strlen($this->{$stream_name});
                if ($remaining > 0) {
                    $this->{$stream_name} .= substr($chunk, 0, $remaining);
                }
            }
        });
    }

    public function change_visibility_window(int $new_window): Future
    {
        print "Extending visibility window for task ID: ".$this->id." from ".$this->visibility_window." to $new_window\n";
        return async(fn () => static::$sqs_client->changeMessageVisibilityAsync([
            'QueueUrl' => static::$queue_url,
            'ReceiptHandle' => $this->receipt_handle,
            'VisibilityTimeout' => $new_window,
        ]));
    }

    protected function schedule_visibility_extension(): void
    {
        if (!$this->terminating) {
            $this->visibility_extension_timer = EventLoop::delay(max(0.01, $this->visibility_window / 2), fn () => $this->extend_visibility_window());
        }
    }

    public function extend_visibility_window()
    {
        $this->visibility_extension_timer = null; // TODO - should we do something more 'formal' to delete this timer?
        if ($this->terminating) {
            return;
        }
        $this->visibility_update = async(function (): void {
            try {
                $new_window = min(43200, max(1, $this->visibility_window * 2));
                $this->change_visibility_window($new_window)->await(); // await() the response before you change the array
                $this->visibility_window = $new_window;
            } catch (\Throwable $error) {
                print "Error changing visibility window: $error\n";
            } finally {
                $this->schedule_visibility_extension();
            }
        });
    }

    public function deleteQueuedMessage(): Future
    {
        if (!$this->receipt_handle) {
            return Future::error(new \Exception("No valid receipt handle; cannot delete"));
        }
        return async(function (): void {
            while (true) {
                try {
                    static::$sqs_client->deleteMessageAsync([
                        'QueueUrl' => static::$queue_url,
                        'ReceiptHandle' => $this->receipt_handle,
                    ]);
                    $this->receipt_handle = null;
                    return;
                } catch (\Throwable $error) {
                    if ($this->delete_retries++ >= self::MAX_RETRIES) {
                        throw $error;
                    }
                    // Keep the completion future pending until deletion has settled.
                    \Amp\delay(5 * $this->delete_retries);
                }
            }
        });
    }
}
