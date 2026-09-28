<?php
declare(strict_types=1);

namespace Massdriver\AmpAws;

use GuzzleHttp\Promise\TaskQueueInterface;
use GuzzleHttp\Promise\Utils;
use Revolt\EventLoop;

/** Schedule SDK continuations only when work exists; never poll. */
class AmpGuzzleTaskQueue implements TaskQueueInterface
{
    protected bool $scheduled = false;
    protected bool $running = false;

    public static function install(): self
    {
        $previous = Utils::queue();
        if ($previous instanceof self) {
            return $previous;
        }
        $queue = new self();
        ;
        if (!$previous->isEmpty()) {
            $queue->add($previous->run(...));
        }
        return $queue;
    }

    public function isEmpty(): bool
    {
        return $this->scheduled === false;
    }

    public function add(callable $task): void
    {
        EventLoop::queue($task); //'queue' is not 'nextTick' but at the _end_ of this tick? Let's try it
        $this->schedule();
    }

    private function schedule(): void
    {
        if ($this->scheduled || $this->running || $this->isEmpty()) {
            return;
        }
        $this->scheduled = true;
        EventLoop::queue(function (): void {
            $this->scheduled = false;
            $this->run();
        });
    }

    public function run(): void
    {
        print "WARNING: synchronous-ish method has been invoked?\n";
        EventLoop::run();
    }
}
