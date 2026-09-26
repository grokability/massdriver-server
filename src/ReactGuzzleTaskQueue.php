<?php
declare(strict_types=1);

namespace Massdriver;

use GuzzleHttp\Promise\TaskQueueInterface;
use GuzzleHttp\Promise\Utils;
use React\EventLoop\LoopInterface;

/** Schedules Guzzle continuations on demand, without polling the task queue. */
class ReactGuzzleTaskQueue implements TaskQueueInterface
{
    private bool $will_fire_next_tick = false;
    private bool $is_running = false;

    private function __construct(
        private LoopInterface $loop,
        private TaskQueueInterface $queue,
    ) {
        // Preserve callbacks queued before installation as well.
        $this->schedule();
    }

    /** Install once, before creating SDK clients. Guzzle's queue is process-wide. */
    public static function install(LoopInterface $loop): self
    {
        $queue = Utils::queue();
        if ($queue instanceof self) {
            if ($queue->loop !== $loop) {
                throw new \LogicException('Guzzle is already attached to a different React loop');
            }
            return $queue;
        }
        $new_queue = new self($loop, $queue);
        Utils::queue($new_queue);
        return $new_queue;
    }

    public function isEmpty(): bool
    {
        return $this->queue->isEmpty();
    }

    public function add(callable $task): void
    {
        $this->queue->add($task);
        $this->schedule();
    }

    public function run(): void
    {
        // Keep explicit run() usable for Guzzle's synchronous wait paths,
        // including a wait invoked from within another queued callback.
        $this->is_running = true;
        try {
            $this->queue->run();
            $this->is_running = false;
        } catch (\Throwable $e) {
            // If a raw task threw, remaining work still (maybe) gets another tick.
            print "A Guzzle task threw: $e\nRe-scheduling ";
            $this->is_running = false; //have to call this first otherwise the schedule() command will just return
            $this->schedule();
        }
    }

    private function schedule(): void
    {
        if ($this->will_fire_next_tick || $this->is_running || $this->queue->isEmpty()) {
            return;
        }
        $this->will_fire_next_tick = true;
        $this->loop->futureTick(function (): void {
            $this->will_fire_next_tick = false;
            $this->run();
        });
    }
}
