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

Run `composer test` and `composer lint` with PHP 8.4.1+ for the current lock file.
See [tests/README.md](tests/README.md) for case filters, isolation, and coverage.
The integration suite uses loopback HTTP, dummy AWS keys, fake SQS operations,
real child processes, and temporary tenant files. It never contacts AWS. It checks
SDK signing/retry, Guzzle scheduling, cancellation, timeouts, process output,
failed-job/cron behavior, visibility extension, deletion retries, shutdown, and
Amp filesystem loading. No static-analysis configuration exists in this project.

## Amp migration

- Amp 3 Futures replace React promises in application code. Await results with
  `->await()`; use `map()` / `catch()` for continuations. Futures are not cancellable
  promises: pass a Cancellation to `AmpAws::receiveMessageAsync()` to abort a poll.
- Revolt owns the global event loop. The first loop argument has been removed from
  `MassdriverQueue`, `Task::boot`, and `FederatedClientCredentialsRefresher`.
  `Massdriver\AmpAws\AmpAws` replaces `Massdriver\ReactAws\ReactAws` and takes
  `(string $client_type, array $options = [])`. SDK method names and AWS Results stay
  the same. An optional AmpAws argument at the end of the queue constructor permits
  an isolated test transport.
- Amp HTTP handles sockets and DNS. AWS keeps ownership of signing, service errors,
  and retries. Guzzle promises remain only at the SDK boundary; continuations are
  queued on Revolt without polling. The transport uses `http_handler`, disables
  automatic redirects/retries, preserves HTTP error responses, and cancels delayed
  or active requests. Synchronous Guzzle `wait()` on pending HTTP is unsupported.
- Amp Process handles completion and concurrently drains stdout/stderr. The first
  64 KiB per stream is retained for diagnostics. No application SIGCHLD handler or
  manual pipe watcher remains. Failed tasks release their slots, and final SQS
  updates/deletion retries drain before the event loop exits. In-flight visibility
  extensions finish before a failed task resets visibility to zero.
- Timers and SIGINT/SIGHUP use Revolt callback IDs. Shutdown cancels long polling;
  the loop exits naturally after remaining work completes.
- All React packages and application references are removed. Other pre-existing
  dependency versions were retained.

## Existing limitations / follow-up

The federated credential refresher was non-parsing pseudocode before this change.
Its directory discovery and `.env` reading now use Amp File, honor the configured
directory, and reload on SIGHUP. Its signal does not keep the daemon alive.
**It does not issue, renew, or write STS credentials.** Calling `run()` explicitly
throws until tenant policy, token duration, retry behavior, and safe file updates
are designed. Do not rely on it for credential renewal.

AWS credential providers can still block during credential discovery/refresh, as
can the startup STS identity check. Those paths were not redesigned here.
The existing `--dev` flag is still not wired into Task failure handling.
Live AWS, TLS/proxy deployment settings, and very long jobs approaching SQS's
12-hour visibility limit still need deployment validation. Task state remains
process-wide, so run one MassdriverQueue at a time per PHP process.
