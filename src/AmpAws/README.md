# Amp AWS adapter

`AmpAws` exposes SDK operations as AWS Result values. Use ordinary SDK method names
without `Async`: the adapter awaits internally and returns an AWS `Result`, not an Amp
`Future`. Waiting suspends the current fiber while other event-loop work continues.

Its `AmpHttpHandler` implements the AWS SDK's PSR-7/Guzzle transport boundary using
Amp HTTP. `AmpGuzzleTaskQueue` connects Guzzle continuations to Revolt. No polling timer is used.

Use `new AmpAws('Sqs', $options)` followed by 
`$client->receiveMessage($params, $cancellation)`. Any operation can accept an
Amp `Cancellation` as its last argument to cancel the underlying HTTP request.
Calling `Future::await($cancellation)` on an outer Future alone only cancels that
wait, not the underlying HTTP request.

The adapter carries the token in the custom `@http.massdriver_cancellation` option
to every transport attempt. The HTTP handler combines it with promise cancellation
and request timeouts, so cancellation aborts delayed sends, active requests, and
response body reads even when SDK promise chains cannot propagate `cancel()`.

The constructor automatically installs the process-wide Guzzle task queue. Create
the adapter before issuing SDK requests, and do not replace Revolt's driver or
Guzzle's task queue while requests are in flight.
`AmpHttpHandler` accepts AWS options in its constructor and exposes the completed
configuration through `get_options()`. It supplies the HTTP handler and default
credential provider when omitted, preserving explicit overrides. The default
EC2/ECS credential providers use the configured HTTP transport, including metadata
HTTP error handling.
Credentials are awaited through Amp before invoking an SDK operation, including
renewal, so the SDK's synchronous authentication checks see resolved credentials.
See [DEV_NOTES.md](../../DEV_NOTES.md) for API changes, verification, and remaining
limitations.
