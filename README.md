# Massdriver

Massdriver is a system that allows you to handle queuing on behalf of hundreds of Laravel apps on one single server. 
Right now, the only implementation is via AWS's SQS system, but any queueing system which has a concept of 'metadata'
that is *outside* of the payload should be able to work, once we hook it together.

In your Laravel apps, you will need to install the Massdriver client and configure it in each app's .env file.

Massdriver is not a full Laravel application. But it does use two composer libraries: the AWS SDK, and DotEnv. So you
will need to run `composer install` to install those dependencies.

For the server configuration, see our [.env.example](/.env.example) file. You can supply configuration in Environment 
variables, or in a `.env` file - whichever makes the most sense in your environment.

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

Massdriver is designed to be run under some kind of daemon-management program, like systemd or supervise. It will 
automatically exit after `TIMES_TO_RUN` executions of the main receive loop, or after having run for `DURATION_TO_RUN` 
seconds. Sometimes during testing you might want to fire off massdriver just once, so in that case you can override your `.env`
by prepending `TIMES_TO_RUN=0` or `DURATION_TO_RUN=0` so that it will only make one trip through the loop. It will 
probably work better with regular, non-FIFO queues. And it definitely works much better on a queue
that is configured for long-polling. You can run multiple copies of it to increase performance, at the cost of more 
resource usage. Reducing `TIMES_TO_RUN` or `DURATION_TO_RUN` to very small values _may_ annoy your daemon management
system, and it might stop restarting your massdriver-server.