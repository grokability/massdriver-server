# Developer Notes

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

There are some concerns about credential-renewing (for time-limited credentials such as Instance
Profiles), but actually coding your way out of that is just too awful, so we don't bother. 
The server might hiccup for a second or two when it's trying to renew credentials, but
that won't happen more than once every 8 or so hours, so I think it's fine.

We *don't* use the AWS 'Batch' (deleteMessageBatch(), etc.) commands here. My thinking is
that different processes are all going to finish at different times, and it's just going to
take too much work to 'batch' *all* the visibility windows or *all* of the deletes together
into bundles of 10, without making things *much* more complicated. We still *do* fetch up to
10 (the SQS max) messages at a time (when warranted). We never fetch messages when there are 
no workers available to execute the tasks in them.

Run `php tests/react-guzzle-integration.php` to check queue scheduling and the HTTP/SDK
integration. This uses a loopback HTTP server and dummy AWS keys, including a simulated
retry; it does not contact AWS. One-shot timers are used for request delays, deadlines,
and test timeouts, never for polling the promise queue.
