<?php
declare(strict_types=1);

namespace Massdriver\ReactAws;

use GuzzleHttp\Promise\TaskQueueInterface;
use React\EventLoop\LoopInterface;

class ReactGuzzleTaskQueue implements TaskQueueInterface
{
    protected $have_added_tasks = false;

    public function __construct(
        private LoopInterface $loop,
    ) {
    }

    public function isEmpty(): bool
    {
        return !$this->have_added_tasks;
    }

    public function add(callable $task): void
    {
        $this->have_added_tasks = true;
        $this->loop->futureTick(function () use (&$task){
            $this->have_added_tasks = false; // set this *first* so that if `$task()` needs to toggle it, it can
            $task();
        });
    }

    public function run(): void
    {
        throw new \Exception('nuh-uh.');
    }
}
