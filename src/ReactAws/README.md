# ReactAws

This is designed to be a drop-in replacement for the AWS SDK, that very lightly (and lovingly)
wraps the various Guzzle Promises inside of React Promises. So, theoretically, no one outside
of this directory needs to know *anything* about Guzzle.

Guzzle's promise callbacks are scheduled onto React's event loop by
`Massdriver\ReactGuzzleTaskQueue`. `MassdriverQueue` installs it in its constructor.
Installation is idempotent for the same loop. Because Guzzle's task queue is process-wide,
installing it for a different loop is rejected. Existing queued callbacks are preserved.

The adapter schedules a `futureTick` only when work is queued. No periodic promise
pump is needed, and an empty queue does not keep the loop alive. `ReactHttpHandler`
uses React's Browser for network I/O and exposes a native Guzzle promise to the SDK,
including AWS's transport error format and cancellation of delayed or active requests.
The experimental promise bridge classes are not used by this path. Blocking operations
inside AWS credential providers remain a separate concern; this adapter only schedules
Guzzle promise continuations. The React HTTP handler is intended for async SDK calls,
and explicitly rejects attempts to synchronously wait on pending HTTP.
