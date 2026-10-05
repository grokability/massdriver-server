<?php
declare(strict_types=1);

namespace Massdriver\AmpAws;

use Amp\DeferredCancellation;
use Amp\CompositeCancellation;
use Amp\TimeoutCancellation;
use Amp\Http\Client\HttpClient;
use Amp\Http\Client\HttpClientBuilder;
use Amp\Http\Client\Request;
use GuzzleHttp\Promise\CancellationException as GuzzleCancellationException;
use GuzzleHttp\Promise\Promise as GuzzlePromise;
use GuzzleHttp\Promise\PromiseInterface as GuzzlePromiseInterface;
use GuzzleHttp\Psr7\Response as GuzzleResponse;
use Psr\Http\Message\RequestInterface;
use function Amp\async;
use function Amp\delay;

/** AWS's PSR-7 / Guzzle transport contract, backed by Amp sockets and DNS. */
class AmpHttpHandler
{
    public const string CANCELLATION_OPTION = 'massdriver_cancellation';

    protected HttpClient $client;

    public function __construct()
    {
        $this->client = (new HttpClientBuilder())->followRedirects(0)->retry(0)->build();
    }

    public function __invoke(RequestInterface $request, array $options = []): GuzzlePromiseInterface
    {
        $cancellation = new DeferredCancellation();
        $promise = new GuzzlePromise(
            static function (): void { throw new \LogicException('Await the Amp AWS Future instead of calling Guzzle wait().'); },
            static function () use ($cancellation, &$promise): void {
                $promise->reject(new GuzzleCancellationException('Promise has been cancelled'));
                $cancellation->cancel();
            },
        );
        async(function () use ($request, $options, $promise, $cancellation): void {
            $requestCancellation = $cancellation->getCancellation();
            try {
                if (isset($options[self::CANCELLATION_OPTION])) {
                    $requestCancellation = new CompositeCancellation($requestCancellation, $options[self::CANCELLATION_OPTION]);
                }
                $token = $requestCancellation;
                $token->throwIfRequested();
                if (($options['delay'] ?? 0) > 0) {
                    delay($options['delay'] / 1000, cancellation: $token);
                }
                $timeout = (float) ($options['timeout'] ?? 30); //FIXME - I think this is wrong
                if ($timeout > 0) {
                    $token = new CompositeCancellation($token, new TimeoutCancellation($timeout));
                }
                $transfer = new Request((string) $request->getUri(), $request->getMethod(), (string) $request->getBody());
                $transfer->setHeaders($request->getHeaders());
                $transfer->setTcpConnectTimeout((float) ($options['connect_timeout'] ?? 5));
                $transfer->setTransferTimeout((float) ($options['timeout'] ?? 30));
                $transfer->setInactivityTimeout(30); // FIXME !
                $response = $this->client->request($transfer, $token);
                $body = $response->getBody()->buffer($token);
                if ($promise->getState() === GuzzlePromiseInterface::PENDING) {
                    $promise->resolve(new GuzzleResponse($response->getStatus(), $response->getHeaders(), $body, $response->getProtocolVersion(), $response->getReason()));
                }
            } catch (\Throwable $error) {
                if ($promise->getState() === GuzzlePromiseInterface::PENDING) {
                    if ($requestCancellation->isRequested()) {
                        $promise->reject(new GuzzleCancellationException('Promise has been cancelled'));
                    } else {
                        $promise->reject(['exception' => $error, 'connection_error' => !($error instanceof \InvalidArgumentException)]);
                    }
                }
            }
        }); //->ignore();
        return $promise;
    }
}
