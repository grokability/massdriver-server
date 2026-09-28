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

    protected string $stdout = '';
    protected string $stderr = '';
    protected Process $process_handle;
    protected ?int $status = null;
    protected float $start_time;
    protected ?string $timer = null;
    protected bool $terminating = false;
    protected array $readers = [];
    protected ?Future $visibility_update = null;
    protected int $delete_retries = 0;

    const int MAX_RETRIES = 5;

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

    public static function delete_process(int $index)
    {
        unset(static::$processes[$index]);
    }

    public static function check_all_tasks_for_termination()
    {
        foreach (static::$processes as $id => $process) {
            if ($process->check_for_termination()) {
                $process->do_termination(fn () => static::delete_process($id));
            }
        }
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
        $id = spl_object_id($this);
        static::$processes[$id] = $this;
        $this->start_time = microtime(true);
        $this->add_stream_handlers('stdout');
        $this->add_stream_handlers('stderr');
        $this->schedule_visibility_extension();
        async(function () use ($id): void {
            $this->status = $this->process_handle->join();
            $this->terminating = true;
            if ($this->timer !== null) {
                EventLoop::cancel($this->timer);
                $this->timer = null;
            }
            // Drain both pipes concurrently, including output buffered at exit.
            await($this->readers);
            $this->visibility_update?->await();
            $this->do_termination(fn () => static::delete_process($id));
        })->catch(static function (\Throwable $error) use ($id): void {
            static::delete_process($id);
            static::$queue->there_are_more_slots_available(1);
            getStderr()->write("Task completion failed: $error\n");
        })->ignore();
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
                $remaining = 65536 - strlen($this->{$stream_name});
                if ($remaining > 0) {
                    $this->{$stream_name} .= substr($chunk, 0, $remaining);
                }
            }
        });
    }

    public function change_visibility_window(int $new_window): Future
    {
        return static::$sqs_client->changeMessageVisibilityAsync([
            'QueueUrl' => static::$queue_url,
            'ReceiptHandle' => $this->receipt_handle,
            'VisibilityTimeout' => $new_window,
        ]);
    }

    private function schedule_visibility_extension(): void
    {
        if (!$this->terminating) {
            $this->timer = EventLoop::delay(max(0.01, $this->visibility_window / 2), fn () => $this->extend_visibility_window());
        }
    }

    public function extend_visibility_window()
    {
        $this->timer = null;
        if ($this->terminating) {
            return;
        }
        $this->visibility_update = async(function (): void {
            try {
                $new_window = min(43200, max(1, $this->visibility_window * 2));
                $this->change_visibility_window($new_window)->await();
                $this->visibility_window = $new_window;
            } catch (\Throwable $error) {
                print "Error changing visibility window: $error\n";
            } finally {
                $this->schedule_visibility_extension();
            }
        })->ignore();
    }

    public function check_for_termination(): bool
    {
        return $this->status !== null && !$this->terminating;
    }

    public function deleteQueuedMessage()
    {
        if (!$this->receipt_handle) {
            return;
        }
        async(function (): void {
            try {
                static::$sqs_client->deleteMessageAsync([
                    'QueueUrl' => static::$queue_url,
                    'ReceiptHandle' => $this->receipt_handle,
                ])->await();
                $this->receipt_handle = null;
            } catch (\Throwable $error) {
                if ($this->delete_retries++ < self::MAX_RETRIES) {
                    EventLoop::delay(5 * $this->delete_retries, fn () => $this->deleteQueuedMessage());
                } else {
                    print "Unable to delete message after retries: $error\n";
                }
            }
        })->ignore();
    }

    public function do_termination(\Closure $callback)
    {
        if ($this->status === null) {
            return;
        }
        $this->terminating = true;
        if ($this->timer !== null) {
            EventLoop::cancel($this->timer);
            $this->timer = null;
        }
        if ($this->status === 0 || $this->delete_after_failure) {
            $this->deleteQueuedMessage();
        } else {
            $this->change_visibility_window(0)->catch(static function (\Throwable $error): void {
                print "Could not reset visibility for failed task: $error\n";
            })->ignore();
        }
        // Release slots on every exit path, including failed jobs.
        $callback();
        static::$queue->there_are_more_slots_available(1);
        if ($this->status !== 0) {
            $command = is_array($this->command_to_run) ? implode(' ', $this->command_to_run) : $this->command_to_run;
            getStderr()->write("Error running process: $command; errno: {$this->status}\nSTDOUT:\n{$this->stdout}\nSTDERR:\n{$this->stderr}\n");
        }
    }
}
