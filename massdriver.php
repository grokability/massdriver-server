<?php

require "vendor/autoload.php";

//we have to use 'unsafe' to actually set the environment variables,
// so that AWS will have access to them.
$dotenv = Dotenv\Dotenv::createUnsafeImmutable(__DIR__);
$dotenv->safeLoad();
$dotenv->required(['SQS_QUEUE','AWS_REGION','COMMAND_TEMPLATE']);
$dotenv->required(['TIMES_TO_RUN','DURATION_TO_RUN','MAX_CONCURRENCY','POLL_TIME','MESSAGE_VISIBILITY_TIMEOUT'])->isInteger();

use Aws\Sts\StsClient;
use Massdriver\MassdriverQueue;
use Massdriver\FederatedClientCredentialsRefresher;
use Revolt\EventLoop;

$sts = new StsClient([
    'version' => '2011-06-15',
]);

try {
    $identity = $sts->getCallerIdentity();
    // print_r($identity);
    // print "Session token is: ".$sts->getSessionToken()."\n";
} catch (\Throwable $e) {
    print "Error signing in to AWS - credentials problem?\n";
    print "Error message is:\n ".$e->getMessage().".\nExiting...\n";
    exit(1);
}
$dev_mode = false;
if(!empty($argv[1])) {
    if( ! in_array($argv[1],["--dev",'--help'])) {
        print("Unknown argument: '".$argv[1]."'\n");
        print("Try ".$argv[0]." --help\n");
        exit(1);
    }
    if($argv[1] == "--help") {
        print("You can run this in --dev mode, where it won't set queue entries visibility to zero on failure\n");
        exit(1);
    }
    $dev_mode = true;
}

print("Starting Massdriver...".($dev_mode ? "IN DEV MODE": "")."\n");

print("Selected loop: ".get_class(EventLoop::getDriver())."\n");

//Load tenant credentials using Amp filesystem operations.
$refresher = null;
if(!empty($_ENV['DIRECTORY_OF_ENV_VARS'])) {
    $refresher = new FederatedClientCredentialsRefresher($_ENV['DIRECTORY_OF_ENV_VARS']);
}

$massdriver = new MassdriverQueue(
    $_ENV['SQS_QUEUE'],
    $_ENV['MAX_CONCURRENCY'],
    $_ENV['TIMES_TO_RUN'],
    $_ENV['DURATION_TO_RUN'],
    $_ENV['COMMAND_TEMPLATE'],
    $_ENV['CRON_TEMPLATE'] ?? '',
    $_ENV['MESSAGE_VISIBILITY_TIMEOUT'],
    $_ENV['POLL_TIME']
);

try {
    [$iterations, $duration] = $massdriver();
} finally {
    $refresher?->close();
}

print("Exiting run - final number of iterations: $iterations, final duration of run: $duration\n");
