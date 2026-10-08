<?php
declare(strict_types=1);

namespace Massdriver\AmpAws;

use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\CompositeCancellation;
use Amp\TimeoutCancellation;
use Amp\Http\Client\HttpClient;
use Amp\Http\Client\HttpClientBuilder;
use Amp\Http\Client\Request;
use Aws\AwsClient;
use Aws\Credentials\CredentialProvider;
use GuzzleHttp\Exception\RequestException as GuzzleRequestException;
use GuzzleHttp\Promise\CancellationException as GuzzleCancellationException;
use GuzzleHttp\Promise\Promise as GuzzlePromise;
use GuzzleHttp\Promise\PromiseInterface as GuzzlePromiseInterface;
use GuzzleHttp\Promise\RejectedPromise as GuzzleRejectedPromise;
use GuzzleHttp\Psr7\Response as GuzzleResponse;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use function Amp\async;
use function Amp\delay;

/** AWS's PSR-7 / Guzzle transport contract, backed by Amp sockets and DNS. */
class AmpHttpHandler
{
    public const string CANCELLATION_OPTION = 'massdriver_cancellation';

    protected HttpClient $client;
    protected array $options;

    public function __construct(array $options = [])
    {
        $this->client = (new HttpClientBuilder())->followRedirects(0)->retry(0)->build();
        $options['http_handler'] ??= $this;
        $options['credentials'] ??= $this->credential_provider($options);
        $this->options = $options;
    }

    public function get_options(): array
    {
        return $this->options;
    }

    public static function credentials_future(AwsClient $client): Future
    {
        $credentials = new DeferredFuture();
        $client->getCredentials()->then(
            static fn () => $credentials->complete(),
            static function ($reason) use ($credentials): void {
                $credentials->error($reason instanceof \Throwable ? $reason : new \RuntimeException('AWS credential discovery failed'));
            },
        );
        return $credentials->getFuture();
    }

    protected function credential_provider(array $options): callable
    {
        // Role providers use "client", independently of the service HTTP handler.
        $http_handler = $options['http_handler'];
        $options['client'] = static function (RequestInterface $request, array $http_options) use ($http_handler): GuzzlePromiseInterface {
            // The important piece here is that we are using *ourselves* to resolve the credentials - hence the `$http_handler` bit
            // that is going to call `__invoke()` below
            return $http_handler($request, $http_options)->then(static function (ResponseInterface $response) use ($request) {
                // Metadata providers expect Guzzle's HTTP error behavior.
                if ($response->getStatusCode() >= 400) {
                    return new GuzzleRejectedPromise([
                        'exception' => GuzzleRequestException::create($request, $response),
                        'response' => $response,
                        'connection_error' => false,
                    ]);
                }
                return $response;
            });
        };
        return CredentialProvider::defaultProvider($options);
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
