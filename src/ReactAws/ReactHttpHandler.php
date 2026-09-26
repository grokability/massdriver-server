<?php
declare(strict_types=1);

namespace Massdriver\ReactAws;

use GuzzleHttp\Promise\Promise as GuzzlePromise;
use GuzzleHttp\Promise\CancellationException as GuzzleCancellationException;
use GuzzleHttp\Promise\PromiseInterface as GuzzlePromiseInterface;
use Psr\Http\Message\RequestInterface;
use React\EventLoop\LoopInterface;
use React\Http\Browser;
use React\Socket\Connector;

/** AWS http_handler contract, backed by React sockets rather than cURL ticking. */
class ReactHttpHandler // I would definitely *feel* better if we had an actual 'interface' to say that we've implemented this right
{
    private Browser $browser;

    public function __construct(
        private LoopInterface $loop
    ) {
        $connector = new Connector(['timeout' => 5.0], $loop);
        //it *looks* like that Connector object uses the cool async DNS component, so I think we're good here?
        $this->browser = new Browser($connector, $loop)
            ->withFollowRedirects(false)->withRejectErrorResponse(false);
    }

    public function __invoke(RequestInterface $request, array $options = []): GuzzlePromiseInterface
    {
        $timer = null;
        $transfer = null;
        $promise = new GuzzlePromise(
            static function (): void { throw new \LogicException('Run the event loop instead of waiting on HTTP.'); },
            function () use (&$timer, &$transfer, &$promise): void {
                // React may reject immediately during cancel(). Settle first so
                // this intentional abort cannot become a retryable network error.
                $promise->reject(new GuzzleCancellationException('Promise has been cancelled'));
                if ($timer !== null) { $this->loop->cancelTimer($timer); }
                if ($transfer !== null) { $transfer->cancel(); }
            }
        );
        $send = function () use ($request, $options, $promise, &$transfer, &$timer): void {
            $timer = null;

            if ($promise->getState() !== GuzzlePromiseInterface::PENDING) {
                return;
            }

            try {
                $browser = $this->browser->withTimeout((float) ($options['timeout'] ?? 30));
                $transfer = $browser->request(
                    $request->getMethod(), (string) $request->getUri(),
                    $request->getHeaders(), (string) $request->getBody()
                );
                $transfer->then(
                    static function ($response) use ($promise): void {
                        if ($promise->getState() === GuzzlePromiseInterface::PENDING) {
                            $promise->resolve($response);
                        }
                    },
                    static function (\Throwable $error) use ($promise): void {
                        if ($promise->getState() === GuzzlePromiseInterface::PENDING) {
                            $promise->reject([
                                'exception' => $error,
                                'connection_error' => !($error instanceof \InvalidArgumentException),
                            ]);
                        }
                    }
                );
            } catch (\Throwable $error) {
                if ($promise->getState() === GuzzlePromiseInterface::PENDING) {
                    $promise->reject(['exception' => $error, 'connection_error' => false]);
                }
            }
        };
        if (($options['delay'] ?? 0) > 0) {
            $timer = $this->loop->addTimer($options['delay'] / 1000, $send);
        } else {
            $this->loop->futureTick($send);
        }
        return $promise;
    }
}
