<?php

require "vendor/autoload.php";
\Amp\File\filesystem(
    new \Amp\File\Driver\BlockingFilesystemDriver()
);
//we *HAVE* to do this otherwise it spawns a bunch of filesystem workers - which we don't need

//we have to use 'unsafe' to actually set the environment variables,
// so that AWS will have access to them.
$dotenv = Dotenv\Dotenv::createUnsafeImmutable(__DIR__);
$dotenv->safeLoad();
$dotenv->required(['SQS_QUEUE','AWS_REGION','COMMAND_TEMPLATE']);
$dotenv->required(['MAX_CONCURRENCY'])->isInteger();

function get_env_var_with_default($env_var): string
{
    // an idea I had - it would be very neat if we could somehow just "not send" parameters that are unset, programmatically.
    // and have the 'defaults' be the constructor's default arguments.
    // Unfortunately, I can't think of how to do the former without a bunch of weird, gross stuff. So maybe we can't?
    // or maybe we have to have the constructors take a dumb, boring array thing. Blech.
    $optional_env_vars = [
        'POLL_TIME' => -1,
        'MESSAGE_VISIBILITY_TIMEOUT' => -1,
        'TIMES_TO_RUN' => 1000,
        'DURATION_TO_RUN' => 3600,
        'CREDENTIAL_DESIRED_DURATION' => 129_600, //as of 9/2026, this is the max allowed in STS/IAM
        'CREDENTIAL_REFRESH_THRESHOLD' => 3600,
    ];

    if(!isset($_ENV[$env_var])) {
        if($optional_env_vars[$env_var] === -1) {
            print "Defaulting to 'LEARN' for env var: $env_var\n";
        } else {
            print "Defaulting `$env_var` to " . $optional_env_vars[$env_var] . "\n";
        }
    }
    return $_ENV[$env_var] ?? $optional_env_vars[$env_var] ?? throw new \Exception("Missing $env_var environment variable, and no default is available");
}

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
    if(!empty($_ENV['INLINE_ROLE_QUOTES_TO_APOSTROPHES']) && !empty($_ENV['INLINE_ROLE'])) {
        throw new \Exception("Cannot have INLINE_ROLE_QUOTES_TO_APOSTROPHES and INLINE_ROLE both in `.env");
    }
    $inline_role = '';
    if(!empty($_ENV['INLINE_ROLE'])) {
        $inline_role = $_ENV['INLINE_ROLE'];
    }
    if(!empty($_ENV['INLINE_ROLE_QUOTES_TO_APOSTROPHES'])) {
        $inline_role = str_replace("'",'"',$_ENV['INLINE_ROLE_QUOTES_TO_APOSTROPHES']);
    }
    $arns = [];
    if(!empty($_ENV['ROLE_ARNS'])) {
        $arns = explode(",", $_ENV['ROLE_ARNS']);
        $arns = array_filter($arns); // yank out 'empty' arrays like [""] which you get by default from explode, above :/
    }
    $refresher = new FederatedClientCredentialsRefresher($_ENV['DIRECTORY_OF_ENV_VARS'], $inline_role, $arns, get_env_var_with_default('CREDENTIAL_DESIRED_DURATION'), get_env_var_with_default('CREDENTIAL_REFRESH_THRESHOLD'));
}

$massdriver = new MassdriverQueue(
    $_ENV['SQS_QUEUE'],
    $_ENV['MAX_CONCURRENCY'],
    get_env_var_with_default('TIMES_TO_RUN'),
    get_env_var_with_default('DURATION_TO_RUN'),
    $_ENV['COMMAND_TEMPLATE'],
    $_ENV['CRON_TEMPLATE'] ?? '',
    get_env_var_with_default('MESSAGE_VISIBILITY_TIMEOUT'),
    get_env_var_with_default('POLL_TIME'),
);

if($refresher) {
    $massdriver->register($refresher);
}

try {
    [$iterations, $duration] = $massdriver();
} finally {
    $refresher?->close();
}

print("Exiting run - final number of iterations: $iterations, final duration of run: $duration\n");
