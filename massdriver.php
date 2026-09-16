<?php

require "vendor/autoload.php";

use Aws\Exception\AwsException;
use Aws\Sqs\SqsClient;

//we have to use 'unsafe' to actually set the environment variables,
// so that AWS will have access to them.
$dotenv = Dotenv\Dotenv::createUnsafeImmutable(__DIR__);
$dotenv->safeLoad();

$client = new SqsClient([
    'version' => '2012-11-05'
]);
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
if(!function_exists('pcntl_sigtimedwait')) {
    print("Shimming in terrible 'pcntl' solution for local development! DO NOT USE THIS IN PRODUCTION!\n");
    function pcntl_sigtimedwait($ignore1,$ignore2,$seconds,$microseconds = 0) {
        usleep($seconds * 1000 + $microseconds);
        return true;
    }
}

$start = microtime(true);
$iterations = 0;
$duration = null;
$number_of_empties = 0;
$oldMask = null;
do {
    $iterations++;
    try {
        $results = $client->receiveMessage([
            'AttributeNames' => ['SentTimestamp'],
            'MaxNumberOfMessages' => (int)$_ENV['MAX_MESSAGES'],
            // 'MessageAttributeNames' => ['All'], // 'MessageGroupId' is the magic parameter!
            'MessageSystemAttributeNames' => ['All'],
            'QueueUrl' => $_ENV['SQS_QUEUE'],
            'WaitTimeSeconds' => (int)$_ENV['POLL_TIME'],
            'VisibilityTimeout' => (int)$_ENV['MESSAGE_VISIBILITY_TIMEOUT'],
        ]);
        if(empty($results->get('Messages'))) {
            $number_of_empties++;
            if($number_of_empties % 3 == 0) {
                print($number_of_empties . " empty message(s) received.\n");
            }

            continue;
        }
        if($number_of_empties) {
            print($number_of_empties . " empty message(s) received before getting a non-empty result\n");
            $number_of_empties = 0;
        }
        // print("Result count is: ".$results->count()."\n");
        // print($results['Messages']);
        foreach($results->get('Messages') as $message) {
            print_r($message);
            try {
                $payload = json_decode($message['Body'], true,8, JSON_THROW_ON_ERROR);

                // The actual job object is base64+serialize()'d in 'command'
                $job = $payload['data']['command'];
                $job_escaped = escapeshellarg("'" . $job . "'");

                $replacements = [
                    '{TENANT}' => $message['Attributes']['MessageGroupId'],
                    '{PAYLOAD}' => escapeshellarg($job),
                    '{B64PAYLOAD}' => base64_encode($job)
                ];
                $command_to_run = str_replace(array_keys($replacements), array_values($replacements), $_ENV['COMMAND_TEMPLATE']);
                print "COMMAND TO RUN IS:\n$command_to_run\n";

                $stdout = tempnam("/tmp", "massdriver-stdout");
                $stderr = tempnam("/tmp", "massdriver-stderr");

                $descriptor_spec = [
                    0 => ['file', '/dev/null','r'],
                    1 => ['file', $stdout,'w'],
                    2 => ['file', $stderr,'w'],
                ];
                $pipes = [];
                // TODO - split the $command_to_run into an array to skip an exec() of 'sh'
                if($oldMask) {
                    if(!pcntl_sigprocmask(SIG_SETMASK, $oldMask)) {
                        throw new RuntimeException("Unable to re-set original signals mask");
                    }
                }
                $exec_results = proc_open($command_to_run, $descriptor_spec, $pipes);
                if (!pcntl_sigprocmask(SIG_BLOCK, [SIGCHLD], $oldMask)) {
                    throw new RuntimeException('Cannot block SIGCHLD');
                }
                $start_time = microtime(true);
                $validity_window = $_ENV['MESSAGE_VISIBILITY_TIMEOUT'];
                if(!$exec_results) {
                    throw new RuntimeException("Could not run command: $command_to_run");
                }

                $process_status = null;
                do {
                    $current_time=microtime(true);
                    $elapsed_time = $current_time - $start_time;
                    $wait_threshold = 0;
                    //if more than half of the Visibility Window has expired, double it.
                    if($elapsed_time > $validity_window/2) {
                        $wait_threshold = $validity_window;
                        $validity_window *= 2;
                        print("Extending deadline - current validity window is ".($validity_window/2)." and $elapsed_time seconds have elapsed. Extending to $validity_window\n");
                        $client->changeMessageVisibilityAsync([
                            'VisibilityTimeout' => (int)$validity_window,
                            'QueueUrl' => $_ENV['SQS_QUEUE'],
                            'ReceiptHandle' => $message['ReceiptHandle'],
                        ]);
                    }
                    $info=[]; //we generally don't actually use this, FYI.
                    $signal = pcntl_sigtimedwait([SIGCHLD], $info, $wait_threshold); //wait for $wait_threshhold seconds, but wake up earlier if needed
                    if ($signal === false || $signal === -1) {
                        $error = pcntl_get_last_error();

                        if ($error !== PCNTL_EAGAIN && $error !== PCNTL_EINTR) {
                            throw new RuntimeException(pcntl_strerror($error));
                        }
                    }
                    $process_status = proc_get_status($exec_results);
                } while ($process_status['running']);

                print("Finished executing command to run! status is: ".print_r($process_status,true)."\n");
                $return_var = $process_status['exitcode']; //depending on the version of PHP, this *might* only be able to be fetched once?
                if ($return_var === 0) {
                    unlink($stdout);
                    unlink($stderr);
                    $delete_results = $client->deleteMessage([
                        'QueueUrl' => $_ENV['SQS_QUEUE'],
                        'ReceiptHandle' => $message['ReceiptHandle'],
                    ]);
                    print("Maybe we deleted something? $delete_results\n");
                } else {
                    print("Execution failed :/ - exit code: $return_var\n");
                    print("STDOUT:\n");
                    fpassthru($pipes[1]);
                    print("STDERR:\n");
                    fpassthru($pipes[2]);
                }
                proc_close($exec_results);
            } catch (\RuntimeException $e) {
                print("RUNTIME exception caught: $e - not removing job.\n");
            } catch(\Exception | JsonException $e) {
                // this would be things like: malformatted $payload, failed json_decode, missing 'command' element, and stuff like that
                // we'll *never* be able to process those messages, so we just make them immediately visible so they will get rapidly
                // eaten up
                print("Exception caught! $e\n");
                if(!$dev_mode) {
                    $client->changeMessageVisibilityAsync([
                        'QueueUrl' => $_ENV['SQS_QUEUE'],
                        'ReceiptHandle' => $message['ReceiptHandle'],
                        'VisibilityTimeout' => 0
                    ]);
                }
            }
        }

    } catch (AwsException $e) {
        // output error message if fails
        print "AWS Exception: $e\n";
        error_log($e->getMessage());
    } catch (\Throwable $e) {
        print "General Exception: $e\n";
        error_log($e->getMessage());
    } finally {
        $duration = microtime(true) - $start;
    }
} while($duration < $_ENV['DURATION_TO_RUN'] && $iterations < $_ENV['TIMES_TO_RUN']);

print("Exiting run - final number of iterations: $iterations, final duration of run: $duration\n");
