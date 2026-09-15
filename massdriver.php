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

$start = microtime(true);
$iterations = 0;
$duration = null;
$number_of_empties = 0;
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
                $payload = json_decode($message['Body'], true);

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

                $descriptor_spec = [
                    0 => ['file', '/dev/null','r'],
                    1 => ['pipe', 'w'], // TODO - something with a lot of output is going to fail
                    2 => ['pipe', 'w'] // same here
                ];
                // TODO - split the $command_to_run into an array to skip an exec() of 'sh'
                $exec_results = proc_open($command_to_run, $descriptor_spec, $pipes);
                $start_time = microtime(true);
                $validity_window = $_ENV['MESSAGE_VISIBILITY_TIMEOUT'];
                if(!$exec_results) {
                    throw new RuntimeException("Could not run command: $command_to_run");
                }

                do {
                    $current_time=microtime(true);
                    $elapsed_time = $current_time - $start_time;
                    if($elapsed_time > $validity_window/2) {
                        print("Extending deadline - current validity window is $validity_window and $elapsed_time seconds have elapsed. Extending to ".($validity_window*2)."\n");
                        $validity_window *= 2;
                        $client->changeMessageVisibilityAsync([
                            'VisibilityTimeout' => (int)$validity_window,
                            'QueueUrl' => $_ENV['SQS_QUEUE'],
                            'ReceiptHandle' => $message['ReceiptHandle'],
                        ]);
                    }
                    usleep(100_000); //0.1 second? I hate this though. It's going to eat more CPU than I would like.
                    //if more than half of the Visibility Window has expired, double it.
                    $process_status = proc_get_status($exec_results);
                } while ($process_status['running']);
                print("Executing command to run! status is: ".print_r($process_status,true)."\n");
                if (!$process_status['running']) {
                    $return_var = $process_status['exitcode']; //depending on the version of PHP, this *might* only be able to be fetched once?
                    if ($return_var === 0) {
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
                }
            } catch(\Exception $e) {
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
