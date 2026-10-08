<?php
declare(strict_types=1);

namespace Massdriver\AmpAws;

use Amp\DeferredFuture;
use GuzzleHttp\Promise\TaskQueueInterface as GuzzleTaskQueueInterface;
use Revolt\EventLoop;
use function Amp\Future\await;

class AmpGuzzleTaskQueue implements GuzzleTaskQueueInterface
{
    protected array $futures = [];

    public function isEmpty(): bool
    {
        return count($this->futures) == 0;
    }

    public function add(callable $task): void
    {
        // That 'callable' is part of *Guzzle* so I need to make my own
        // Future to wrap it, so I can wait on it later (if I really need to).
        $deferred = new DeferredFuture();
        $this->futures[] = $deferred->getFuture();
        $my_index = array_key_last($this->futures);
        EventLoop::queue(function () use ($deferred, $task, $my_index): void {
            unset($this->futures[$my_index]);
            try {
                $task();
                $deferred->complete();
            } catch (\Throwable $exception) {
                $deferred->error($exception);
            }
        }); //'queue' is not 'nextTick' but at the _end_ of this tick? Let's try it
    }

    public function run(): void
    {
        // SDK constructors can call run() from inside a Guzzle callback. The
        // running callbacks cannot finish until run() returns; only drain work
        // still queued, including work added by the callbacks we await.
        while (count($this->futures) > 0) {
            await($this->futures); // The futures we await could add *other* futures, and we need to make sure *all* have run
        }
    }
}
