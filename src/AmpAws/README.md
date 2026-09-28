# Amp AWS adapter

`AmpAws` exposes SDK operations as Amp Futures, preserving AWS Result values.
Its `AmpHttpHandler` implements the AWS SDK's PSR-7/Guzzle transport boundary using
Amp HTTP. `AmpGuzzleTaskQueue::install()` connects SDK continuations to Revolt and
is idempotent; pre-existing queued work is preserved. No polling timer is used.

Use `new AmpAws('Sqs', $options)` followed by
`$client->receiveMessageAsync($params, $cancellation)->await()`.
Unsuffixed operation names also execute via the SDK's async path.
Cancel receives using an Amp Cancellation; calling `Future::await($cancellation)`
alone only cancels the wait, not the underlying HTTP request.

The queue is process-wide. Install it before issuing SDK requests, and do not
replace Revolt's driver or Guzzle's task queue while requests are in flight.
See `DEV_NOTES.md` for API changes, verification, and remaining limitations.
