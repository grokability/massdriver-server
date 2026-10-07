# Massdriver

Massdriver is a system that allows you to handle various mass-hosting related things on behalf of hundreds of Laravel 
apps on one single server. The two main elements are queueing, and credential refreshing.

A challenge with hosting many Laravel installs on one machine is that you probably don't want to be running one regular
"Queue-Runner" for each tenant; that would be extremely wasteful of system resources. Massdriver uses a single shared
queue per-server to handle all Laravel queuing for all the tenants.

Additionally, scheduled tasks in Laravel can be a bit of a nightmare to manage in a mass-hosting environment. One way
you can handle that is to just use a regular `crontab` for each tenant, and try to prevent too many cron tasks from
all firing off at the same time. But with Massdriver's Shared Queue, you can make each of those cron jobs fire off a tiny 
shell script, which _also_ feeds into SQS, and runs similarly to how Massdriver runs Laravel's Queued jobs.

Finally, when trying to grant access to S3 buckets and other AWS resources, you probably *don't* want to create
an IAM user for every user on your shared-hosting box. And you probably *don't* want your tenants to share broad access
to various resources. The FederatedCredentialsRefresher creates temporary "federated" AWS Credentials for each tenant, 
and refreshes those credentials as-needed.

Massdriver uses the AWS SDK, DotEnv, and Amp on the Revolt event loop. Run
`composer install` to install the locked dependencies. The source requires PHP 8.3+;
the current lock file requires PHP 8.4.1+ (including its existing Symfony dependency).

For the server configuration, see our [.env.example](/.env.example) file. You can supply configuration in Environment
variables, or in a `.env` file - whichever makes the most sense in your environment. You can also set AWS variables like
`AWS_PROFILE`, `AWS_REGION`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` and they'll be made available to the SQS
Client constructor. The default AWS credential resolution works the same as in the regular AWS SDK for PHP, so instance 
profiles and the like will continue to work the same way as before.

Massdriver is designed to be run under some kind of daemon-management program, like systemd or supervise. It will
automatically exit after `TIMES_TO_RUN` executions of the main receive loop, or after having run for `DURATION_TO_RUN`
seconds, with plenty of wiggle-room for long-running tasks (so don't get too attached to that duration). Sometimes
during testing you might want to fire off Massdriver just once, so in that case you can override your `.env`
by prepending `TIMES_TO_RUN=1` or `DURATION_TO_RUN=1` to your command-line invocation so that it will only make one
trip through the loop. It will probably work better with regular, non-FIFO queues. And it definitely works much better on a queue
that is configured for long-polling. You can run multiple copies of it to increase performance, at the cost of more
resource usage. But, more likely, you will just instead increase `MAX_CONCURRENCY`. Reducing `TIMES_TO_RUN` or 
`DURATION_TO_RUN` to very small values _may_ annoy your daemon management system, and it might stop restarting your 
massdriver-server.

## SharedQueue

Right now, the only Queueing implementation is via AWS's SQS system, but any queueing system which has a concept of 'metadata'
that is *outside* of the payload should be able to work, once we hook it together.

In your Laravel apps, you will need to install the [Massdriver client](https://github.com/grokability/massdriver) and 
configure it in each app's `.env` file.

The main part to pay attention to is `COMMAND_TEMPLATE`. This will be the 
actual command that gets run on behalf of your Laravel apps. The `{TENANT}` identifier will be swapped out with whatever
'tenant' information your laravel apps sent. The `{PAYLOAD}` identifier is the PHP-serialized Laravel job that was submitted to
the queue. Because the quoting inside that `{PAYLOAD}` variable can be extremely nasty to try to untangle, we also can
use the `{B64PAYLOAD}` instead, which is the base64-encoded version of the serialized PHP job information.

A common `COMMAND_TEMPLATE` `.env`-var setting might be something along the lines of:

```dotenv
COMMAND_TEMPLATE="run-as-user {TENANT} php artisan tinker --execute='$job = unserialize(base64_decode(\"{B64PAYLOAD}\"));
resolve(Illuminate\\Contracts\\Bus\\Dispatcher::class)->dispatchNow($job);'"
```

But you have to figure out how that will work in your environment.

When trying to figure out your `COMMAND_TEMPLATE`, you can run in `--dev` mode where it won't immediately mark SQS
messages that cause an Exception as immediately re-available, which could force SQS messages into a Dead-Letter Queue.

For 'Cron Mode', use something like [our SQS pusher script](./sqs_sender.sh) to push the entire command up into SQS. 
For example, if you had something like this in your cron:

```crontab
23 7 * * * do_a_command_here param1 param2 param3
```
you would replace that with:

```crontab
23 7 * * * sqs_sender.sh do_a_command_here param1 param2 param3 
```
The `COMMAND_TEMPLATE` environment variable shows how commands will be passed through to the system. The simplest way to
do that would be to set that to `{COMMANDLINE}` (which will swap in the commandline that was pushed up to SQS).

The `MESSAGE_VISIBILITY_TIMEOUT` environment variable is very important - it determines how long the initial message 
should take to complete (approximately). If not set, it will be detected by querying the SQS queue's attributes. 
The process-management loop will evaluate if the process is still running, and 
if there is less than half of the Visibility Timeout, it will double it. For example, let's take a `MESSAGE_VISIBILITY_TIMEOUT` of 30 seconds
(which is SQS's default). After the resulting process has run for 15 seconds, Massdriver will then extend the timeout by
60 seconds (total duration: 85 seconds). And then, after 30 more seconds, it will extend by 120, and so on. Using this 
algorithm, very long-duration tasks can still complete successfully, and short-lived tasks can still be executed quickly.

## FederatedClientCredentialsRefresher

This optional component will take a directory of Laravel installs (`DIRECTORY_OF_ENV_VARS` in the `.env`), and create 
or refresh "federated" credentials for each, manually writing them back into each `.env` file. The permissions for the 
federated credentials will be based on `INLINE_ROLE` or `INLINE_ROLE_QUOTES_TO_APOSTROPHES` (not both) and/or 
`ROLE_ARNS` which is a comma-delimited list of policies that you want to the federated user to be associated with. The
`INLINE_ROLE_QUOTES_TO_APOSTROPHES` `.env` var exists because otherwise, you would have to keep backslashing all of the
quotes inside of your inline role, which is unpleasant to look at, and hard to diagnose. So you can just replace them with
apostrophes instead (`'`). You still *will* have to backslash any dollar signs, however.

You can adjust credential duration and what the appropriate refresh threshold should be. Sending a `SIGHUP` to massdriver
will tell it to re-read the directory of `DIRECTORY_OF_ENV_VARS`.