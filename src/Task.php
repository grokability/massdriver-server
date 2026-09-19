<?php

namespace Massdriver;

use Aws\Sqs\SqsClient;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;
use React\Stream\ReadableResourceStream;
use React\Stream\WritableResourceStream;

class Task
{
    protected static int $max_processes = 0;
    protected static LoopInterface $loop;
    protected static SqsClient $sqs_client;
    protected static string $queue_url;
    protected static mixed $error_stream;

    protected mixed $stdout_handle;
    protected mixed $stderr_handle;
    private string $stdout = '';
    private string $stderr = '';
    private static array $processes = [];
    private mixed $handle;
    protected ?int $status = null;
    protected float $start_time;
    protected ?TimerInterface $timer = null;

    const int BUFFER_SIZE = 65535;

    public static function boot(
        LoopInterface $loop,
        SqsClient $sqs_client,
        string $queue_url,
        int $max_processes,
        mixed $error_stream
    )
    {
        static::$loop = $loop;
        static::$sqs_client = $sqs_client;
        static::$queue_url = $queue_url;
        static::$max_processes = $max_processes;
        static::$error_stream = new WritableResourceStream($error_stream, static::$loop);
        static::$loop->addSignal(
            SIGCHLD,  fn () => static::check_all_tasks_for_termination()  //[static::class,'all'],
        );
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
        foreach (static::$processes as $id => $process) {
            if($process->check_for_termination()) {
                $process->do_termination(fn () => static::delete_process($id));
            }
        }
    }

    public function __construct(
        public string|array $command_to_run,
        public $receipt_handle,
        public int $visibility_window,
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
        $this->handle = proc_open($command_to_run, $descriptor_spec, $pipes);
        if($this->handle === false) {
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
        static::$loop->addReadStream($this->{$stream_name.'_handle'},
            function ($stream) use ($stream_name) {
                if(!$this->{$stream_name}) {
                    $this->{$stream_name} .= fread($stream, static::BUFFER_SIZE);
                } else {
                    if(fseek($this->{$stream_name.'_handle'},0,SEEK_END) !== 0) {
                        // We were unable to 'seek' for whatever reason, so just read the stream and throw it away
                        fread($stream, static::BUFFER_SIZE);
                    }
                }

            }
        );
    }

    public function extend_visibility_window()
    {
        if(!$this->timer) {
            print "Initial boot of timer\n";
            $this->timer = static::$loop->addTimer($this->visibility_window/2,fn() => $this->extend_visibility_window());
        } elseif(!$this->check_for_termination()) {
            $now = microtime(true);
            if($now-$this->start_time > $this->visibility_window/2) {
                $new_visibility_window = $this->visibility_window * 2;
                static::$sqs_client->changeMessageVisibilityAsync([
                    'QueueUrl' => static::$queue_url,
                    'ReceiptHandle' => $this->receipt_handle,
                    'VisibilityTimeout' => $new_visibility_window,
                ])->then(function () use ($new_visibility_window) {
                    $this->visibility_window = $new_visibility_window;
                    $this->timer = static::$loop->addTimer($this->visibility_window/2,fn() => $this->extend_visibility_window());
                })->otherwise(function ($reason) {
                    print "Error changing visibility window for handle: ".$this->receipt_handle.": $reason\n";
                    static::$loop->futureTick(fn () => $this->extend_visibility_window());
                });
            }
        }
    }

    public function __destruct() {
        print "DESTRUCTING a task!";
    }

    public function check_for_termination():bool
    {
        $status = proc_get_status($this->handle);

        if (!$status['running']) {
            $this->status = $status['exitcode'];
            return true;
        }

        return false;
    }

    public function do_termination(\Closure $callback)
    {
        if(is_null($this->status)) {
            print "ERROR - cannot terminate unterminated process!\n";
            return;
        }
        static::$loop->cancelTimer($this->timer);

        proc_close($this->handle);

        static::$loop->removeReadStream($this->stdout_handle);
        static::$loop->removeReadStream($this->stderr_handle);

        if($this->status === 0) {
            static::$sqs_client->deleteMessageAsync([
                'QueueUrl' => self::$queue_url,
                'ReceiptHandle' => $this->receipt_handle,
            ])->then(function ($results) use ($callback) {
                print($results);
                $callback();
                // FIXME - somehow hook into Massdriver to signal it?
            })->otherwise(function ($reason) use ($callback) {
                print "DeleteMessage call failed! Reason: $reason\n";
                // I don't know what to do now? I *guess* just 'try again'? probably needs to a limit in there somewhere though...
                static::$loop->futureTick(fn () => $this->do_termination($callback));
                //FIXME - don't call massdriver yet?
            });
        } else {
            // write things out to tempfiles for later debugging
            static::$error_stream->write('Error running process: '.(is_array($this->command_to_run) ? $this->command_to_run : implode(" ".$this->command_to_run, true))." errno: ".$this->status."\n");
            static::$error_stream->write('STDOUT: \n.'.$this->stdout.'\n');
            static::$error_stream->write('STDERR: \n'.$this->stderr.'\n');
        }
    }
}
