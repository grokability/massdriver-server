# Developer Notes

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
