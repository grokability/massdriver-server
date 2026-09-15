# Massdriver

Massdriver is a system that allows you to handle queuing on behalf of hundreds of Laravel apps on one single server. 
Right now, the only implementation is via AWS's SQS system, but any queueing system which has a concept of 'metadata'
that is *outside* of the payload should be able to work, once we hook it together.

In your Laravel apps, you will need to install the Massdriver client and configure it in each app's `.env` file.

Massdriver is not a full Laravel application. But it does use two composer libraries: the AWS SDK, and DotEnv. So you
will need to run `composer install` to install those dependencies.

For the server configuration, see our [.env.example](/.env.example) file. You can supply configuration in Environment 
variables, or in a `.env` file - whichever makes the most sense in your environment. You can also set AWS variables like 
`AWS_PROFILE`, `AWS_REGION`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` and they'll be made available to the SQS
Client constructor.

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

The `MESSAGE_VISIBILITY_TIMEOUT` environment variable is very important - it determines how long the initial message 
should take to complete (approximately). The process-management loop will evaluate if the process is still running, and 
if there is less than half of the Visibility Timeout, it will double it. For example, let's take a `MESSAGE_VISIBILITY_TIMEOUT` of 30 seconds
(which is SQS's default). After the resulting process has run for 15 seconds, Massdriver will then extend the timeout by
60 seconds (total duration: 85 seconds). And then, after 30 more seconds, it will extend by 120, and so on. Using this 
algorithm, very long-duration tasks can still complete succesfully, and short-lived tasks can still be executed quickly.

Massdriver is designed to be run under some kind of daemon-management program, like systemd or supervise. It will 
automatically exit after `TIMES_TO_RUN` executions of the main receive loop, or after having run for `DURATION_TO_RUN` 
seconds. Sometimes during testing you might want to fire off massdriver just once, so in that case you can override your `.env`
by prepending `TIMES_TO_RUN=0` or `DURATION_TO_RUN=0` to your command-line invocation so that it will only make one 
trip through the loop. It will probably work better with regular, non-FIFO queues. And it definitely works much better on a queue
that is configured for long-polling. You can run multiple copies of it to increase performance, at the cost of more 
resource usage. Reducing `TIMES_TO_RUN` or `DURATION_TO_RUN` to very small values _may_ annoy your daemon management
system, and it might stop restarting your massdriver-server.