<?php
declare(strict_types=1);

namespace Massdriver\AmpAws;

use Amp\DeferredCancellation;
use Amp\CompositeCancellation;
use Amp\TimeoutCancellation;
use Amp\Http\Client\HttpClient;
use Amp\Http\Client\HttpClientBuilder;
use Amp\Http\Client\Request;
use GuzzleHttp\Promise\CancellationException;
use GuzzleHttp\Promise\Promise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Response;
use Psr\Http\Message\RequestInterface;
use function Amp\async;
use function Amp\delay;

/** AWS's PSR-7 / Guzzle transport contract, backed by Amp sockets and DNS. */
final class AmpHttpHandler
{
    private HttpClient $client;

    public function __construct()
    {
        $this->client = (new HttpClientBuilder())->followRedirects(0)->retry(0)->build();
    }

    public function __invoke(RequestInterface $request, array $options = []): PromiseInterface
    {
        $cancellation = new DeferredCancellation();
        $promise = new Promise(
            static function (): void { throw new \LogicException('Await the Amp AWS Future instead of calling Guzzle wait().'); },
            static function () use ($cancellation, &$promise): void {
                $promise->reject(new CancellationException('Promise has been cancelled'));
                $cancellation->cancel();
            },
        );
        async(function () use ($request, $options, $promise, $cancellation): void {
            try {
                $token = $cancellation->getCancellation();
                $token->throwIfRequested();
                if (($options['delay'] ?? 0) > 0) {
                    delay($options['delay'] / 1000, cancellation: $token);
                }
                $timeout = (float) ($options['timeout'] ?? 30);
                if ($timeout > 0) {
                    $token = new CompositeCancellation($token, new TimeoutCancellation($timeout));
                }
                $transfer = new Request((string) $request->getUri(), $request->getMethod(), (string) $request->getBody());
                $transfer->setHeaders($request->getHeaders());
                $transfer->setTcpConnectTimeout((float) ($options['connect_timeout'] ?? 5));
                $transfer->setTransferTimeout((float) ($options['timeout'] ?? 30));
                $response = $this->client->request($transfer, $token);
                $body = $response->getBody()->buffer($token);
                if ($promise->getState() === PromiseInterface::PENDING) {
                    $promise->resolve(new Response($response->getStatus(), $response->getHeaders(), $body, $response->getProtocolVersion(), $response->getReason()));
                }
            } catch (\Throwable $error) {
                if ($promise->getState() === PromiseInterface::PENDING) {
                    $promise->reject(['exception' => $error, 'connection_error' => !($error instanceof \InvalidArgumentException)]);
                }
            }
        })->ignore();
        return $promise;
    }
}
