<?php

namespace Massdriver;

use Amp\File\Filesystem;
use Revolt\EventLoop;
use function Amp\async;
use function Amp\Future\awaitAll;
use function Amp\File\filesystem;

/**
 * Deal with this, Foreman. Be as patient as you can...
 * (Hopefully, this works better than that guy did...)
 */

class Foreperson extends EventLoopTask
{
    protected array $registrations = [];
    protected bool $shutting_down = false;
    protected float $start_time;
    protected array $runners = [];
    const float ACCOUNTING_PERIOD = 60.0; // check every minute to see if tasks are complete

    public static function get_env_vars(): array
    {
        return [
            'max_iterations' => 'TIMES_TO_RUN',
            'max_duration' => 'DURATION_TO_RUN',
            'filesystem_driver' => [
                'FILESYSTEM_DRIVER' => function ($driver) {
                    $filesystem_driver_name = $driver ? '\\Amp\\File\\Driver\\'
                        . ucfirst($driver)
                        . 'FilesystemDriver' : null;

                    return match ($driver ?? 'auto') {
                        'auto' => null,
                        'eio', 'uv' => filesystem(
                            new $filesystem_driver_name(EventLoop::getDriver())
                        ),
                        default => filesystem(
                            new $filesystem_driver_name()
                        ),
                };
            }]
        ];
    }

    public function __construct(
        public int $max_iterations = 1000,
        public int $max_duration = 3600,
        public ?Filesystem $filesystem_driver = null,
    ) {
        //what do we do here? FIXME (if anything)
        // print out loop type? OR at least select what our loop type is or should be?
    }

    public function __invoke():void
    {
        $signal_handler = EventLoop::onSignal(SIGINT, function () {
            if($this->shutting_down) {
                print "Second Interrupt Signal Detected, exiting *NOW*\n";
                exit(1);
            }
            $this->shutting_down = true;
            print "Interrupt Signal Detected! Allowing tasks to finish. (Hit Ctrl+C again to force exit)\n";
            $this->graceful_shutdown();
        });
        EventLoop::unreference($signal_handler);

        $reload_handler = EventLoop::onSignal(SIGHUP, function () {
            foreach($this->registrations as $registration) {
                $registration->reload();
            }
        });
        EventLoop::unreference($reload_handler);

        $this->start_time = microtime(true);
        foreach ($this->registrations as $registration) {
            $this->runners[] = async(fn () => $registration());
        }

        $end_duration = EventLoop::delay($this->max_duration, function () {
            print "Time's up!\n";
            $this->graceful_shutdown();
        });
        EventLoop::unreference($end_duration);

        //
        $end_iterations = EventLoop::repeat(60, function () {
            $iterations_count = $this->get_iterations_count();
            print "Iterations checker returns: $iterations_count - but max iterations are: ".$this->max_iterations."\n";
            if($iterations_count >= $this->max_iterations) {
                print "Iterations are finished! Current count: $iterations_count, max: ".$this->max_iterations."\n";
                $this->graceful_shutdown();
            }
        });
        EventLoop::unreference($end_iterations);

        EventLoop::run();
        //awaitAll($runners); // FIXME - what to do here?!
    }

    public function register(EventLoopTask $system): void
    {
        $this->registrations[] = $system;
    }

    public function graceful_shutdown():void
    {
        print "Foreperson gracefully shutting down...\n";
        foreach($this->registrations as $system) {
            async($system->graceful_shutdown(...)); //TODO - I don't know if I really like this syntax?
        }
    }

    public function get_iterations_count(): int
    {
        $highest_iteration_count = -1;
        foreach ($this->registrations as $index => $subtask) {
            $iteration_count = $subtask->get_iterations_count();
            print "Iteration count is: $iteration_count for Subtask ID: $index\n";
            if($iteration_count > $highest_iteration_count) {
                $highest_iteration_count = $iteration_count;
            }
        }
        return $highest_iteration_count;
    }

    public function get_final_statistics():array {
        return [$this->get_iterations_count(),microtime(true)-$this->start_time];
    }
}