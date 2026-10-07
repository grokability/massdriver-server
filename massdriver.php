<?php

require "vendor/autoload.php";

use Massdriver\Foreperson;

//we have to use 'unsafe' to actually *set* the environment variables,
// so that AWS will have access to them.
$dotenv = Dotenv\Dotenv::createUnsafeImmutable(__DIR__);
$dotenv->safeLoad();
$dotenv->required(['AWS_REGION']);

use Aws\Sts\StsClient;
use Massdriver\SharedQueue;
use Massdriver\FederatedClientCredentialsRefresher;

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

unset($sts);
unset($identity);

$dev_mode = false;
if(!empty($argv[1])) {
    if( ! in_array($argv[1],["--dev",'--help'])) {
        print("Unknown argument: '".$argv[1]."'\n");
        print("Try ".$argv[0]." --help\n");
        exit(1);
    }
    if($argv[1] == "--help") {
        print("You can run this in --dev mode, where it won't set queue entries visibility to zero on failure\n");
        print("Allowed environment variables are: \n\n");
        print("(Supervisor environment variables:)\n\n");
        foreach(Foreperson::get_env_var_names() as $foreperson_var) {
            print "$foreperson_var\n";
        }
        print "\n(Queue-specific environment variables:)\n\n";
        foreach(SharedQueue::get_env_var_names() as $massdriver_var) {
            print "$massdriver_var\n";
        }
        print "\n(Credential-fetcher-specific environment variables:)\n\n";
        foreach(FederatedClientCredentialsRefresher::get_env_var_names() as $fed_cred) {
            print "$fed_cred\n";
        }
        exit(1);
    }
    $dev_mode = true;
}

print("Starting Massdriver...".($dev_mode ? "IN DEV MODE": "")."\n");

$foreperson = new Foreperson(...Foreperson::env_to_constructor_params($_ENV));

$refresher = null;
if(isset($_ENV['DIRECTORY_OF_ENV_VARS'])) {
    $refresher_params = FederatedClientCredentialsRefresher::env_to_constructor_params($_ENV);
    $refresher = new FederatedClientCredentialsRefresher(...$refresher_params);
    $foreperson->register($refresher);
}

$massdriver = null;
if(isset($_ENV['SQS_QUEUE']) && isset($_ENV['MAX_CONCURRENCY']) && isset($_ENV['COMMAND_TEMPLATE'])) {
    $massdriver_parameters = SharedQueue::env_to_constructor_params($_ENV);
    //FIXME - does not respect 'dev_mode'?
    $massdriver = new SharedQueue(...$massdriver_parameters);
    $foreperson->register($massdriver);
}

try {
    $foreperson();
    [$iterations, $duration] = $foreperson->get_final_statistics();
} catch (\Throwable $e) {
    print "Fatal Exception encountered: ".$e->getMessage()."\n";
}

print("Exiting run - final number of iterations: $iterations, final duration of run: $duration\n");
