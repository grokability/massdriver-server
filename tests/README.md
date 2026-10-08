# Tests

Run `composer test` for all cases and `composer lint` for PHP syntax checks.
The current dependency lock requires PHP 8.4.1 or later.

The runner executes each case in a fresh PHP process, reports every failure,
and exits nonzero if any case fails. This isolates process-wide task state,
retry counters, signals and timers. Each case has a 30-second deadline.

To run a subset:

```sh
php tests/run.php integration
php tests/run.php refresher
php tests/run.php visibility-race
php tests/run.php write-failure
php tests/run.php filesystem
```

Tests never load the application's `.env` or contact AWS. The transport case
uses a loopback HTTP server and explicit dummy AWS keys; it needs permission
to bind a local socket. Daemon cases run real PHP child processes with fake SQS
responses. Credential cases use temporary tenant directories and fake STS
responses, including the SDK's actual `DateTimeResult` expiration type.
Most cases use the blocking filesystem driver. The `filesystem/parallel` case
explicitly starts a one-worker pool and shuts it down after testing; this case
needs permission to create a local IPC socket. Both filesystem cases check real
reads, writes, metadata preservation, replacement, and
cleanup after a rendering error. They do not contact AWS.

## Coverage

| Cases | Behavior |
| --- | --- |
| `filesystem/blocking`, `filesystem/parallel` | Filesystem operations honor the selected driver, including metadata, move and failure cleanup |
| `integration/transport` | Guzzle queue ordering, errors and repeated use; promise recovery; AWS signing/retry; HTTP delay, cancellation, timeout and cleanup |
| `integration/queue-reentrant` | A Guzzle callback can drain the queue without waiting on itself, including work added by nested callbacks |
| `integration/deletion-retry` | Receive recovery, large stdout/stderr drainage, deletion retries and natural drain |
| `integration/malformed-batch` | A malformed message does not prevent later messages in the batch from running |
| `integration/failed-jobs` | Failed job visibility reset, failed cron deletion and early pipe closure |
| `integration/visibility` | Long-running job visibility extension |
| `integration/shutdown` | Cancelling an outstanding receive |
| `integration/startup-idle` | Both async subsystems and their queued startup work run before the supervisor exits naturally when idle |
| `integration/sdk-startup` | Real SDK discovery and receive run through SharedQueue and Foreperson without fixture timers keeping the loop alive; HTTP responses use dummy keys and a fake handler |
| `integration/role-credentials` | The full default credential chain reaches EC2 role discovery through the configured async transport, sends the IMDSv2 token, and signs SQS with the resulting role credentials |
| `integration/single-worker` | Releasing the last slot before restarting polling |
| `integration/visibility-race` | An extension already in flight must finish before the failure reset; explicit gates coordinate child exit and extension completion |
| `integration/shutdown-listener`, `supervisor-accounting`, `supervisor-signals` | Foreperson dispatches shutdown once, enforces the accounting limit, and dispatches SIGINT/SIGHUP |
| `refresher/mapping` | Credentials, expiration, inline policy, policy ARNs and tenant tags |
| `refresher/write-success` | Caller starts credential retrieval; writing returns void after replacing/appending keys while preserving unrelated content and file metadata |
| `refresher/write-failure`, `write-error-cleanup` | Original-file preservation and temporary-file cleanup for both Exceptions and Errors |
| `refresher/duplicate-keys` | Duplicate assignments do not override refreshed credentials |
| `refresher/concurrent-refresh`, `retry-reset` | Overlapping renewal/reload requests share work and successful renewal resets backoff |
| `refresher/load` | Loading valid credentials, scheduling renewal and shutdown cleanup |
| `refresher/backoff`, `retry-isolation` | Failure backoff and independent tenant retry budgets |
| `refresher/reschedule` | Reloading newer credentials preserves a future renewal after the old deadline |
| `refresher/shutdown-in-flight` | A renewal finishing during shutdown must not schedule further work |
| `refresher/empty` | Iteration counts stay zero before loading and with no tenants |

Credential fixtures bypass the constructor solely to inject fake STS and avoid
AWS credential discovery. They exercise the real load, refresh, write and shutdown
methods. Constructor/CLI wiring, live AWS permissions, and deployment-specific
filesystem behavior remain outside this suite.

Daemon fixtures register the queue with Foreperson and request shutdown after a
fixed number of fake receive responses, so process tests need not wait for the
supervisor's normal minute-long accounting interval. `supervisor-accounting`
separately exercises that production callback with a shorter interval.

These are assertions of intended behavior, not snapshots of existing bugs. Known
production failures are neither skipped nor marked as expected failures. Syntax
errors in production files will fail affected cases before their assertions run.

Renewal failures retry independently per tenant after 5, 10, 30, 45, then 60
seconds, remaining at 60 seconds until success or shutdown. Message deletion
keeps its task slot until deletion succeeds or its five retries are exhausted.
