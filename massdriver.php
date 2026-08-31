<?php

require "vendor/autoload.php";

use Aws\Exception\AwsException;
use Aws\Sqs\SqsClient;

$dotenv = Dotenv\Dotenv::createImmutable(__DIR__);
$dotenv->safeLoad();

$client = new SqsClient([
    'profile' => 'default',
    'region' => $_ENV['AWS_REGION'],
    'version' => '2012-11-05'
]);
print("Starting Massdriver...");
$start = microtime(true);
$iterations = 0;
$duration = null;
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
        ]);
        if(empty($results->get('Messages'))) {
            print("Empty Queue Receive\n");
            continue;
        }
        // print("Result count is: ".$results->count()."\n");
        // print($results['Messages']);
        foreach($results->get('Messages') as $message) {
            print_r($message);
            if (! isset($message['Attributes']['MessageGroupId'])) {
                // I don't know what to do here but I definitely *don't* know what to do with this message...
                print("I dunno what to do with this message because it has no MessageGroupId\n");
                continue; // or do something else?
            }
            //set timeout based on SetVisibilityTimeout
            //run-as-user $result['MessageGroupId'] - wait for result. Be prepared to fork-many
            // if success, DeleteMessage based on ReceiptHandle

            $payload = json_decode($message['Body'], true);

            // The actual job object is base64+serialize()'d in 'command'
            $job = $payload['data']['command'];
            $job_escaped = escapeshellarg("'".$job."'");

            $replacements = [
                '{TENANT}' => $message['Attributes']['MessageGroupId'],
                '{PAYLOAD}' => escapeshellarg($job),
                '{B64PAYLOAD}' => base64_encode($job)
            ];
            print "Job is: $job\n\nEscaped is: ".escapeshellarg($job)."\n\nWeiredly double-scaped is: $job_escaped\n\n";
            $command_to_run = str_replace(array_keys($replacements), array_values($replacements), $_ENV['COMMAND_TEMPLATE']);
            print "COMMAND TO RUN IS:\n$command_to_run\n";
            print "\nCOMMAND TO RUN - double-escaped - is:\n".escapeshellcmd($command_to_run)."\n";
            $output = null;
            $return_var = null;
            $exec_results = exec($command_to_run,$output,$return_var);
            print("Executed command to run! Result was: $exec_results, with return code: ($return_var)\n");
            if($return_var === 0) {
                $delete_results = $client->deleteMessage([
                    'QueueUrl' => $_ENV['SQS_QUEUE'],
                    'ReceiptHandle' => $message['ReceiptHandle'],
                ]);
                print("Maybe we deleted something? $delete_results\n");
            } else {
                print("Executino failed :/\n");
            }
        }

    } catch (AwsException $e) {
        // output error message if fails
        error_log($e->getMessage());
    } finally {
        $duration = microtime(true) - $start;
    }
} while($duration < $_ENV['DURATION_TO_RUN'] && $iterations < $_ENV['TIMES_TO_RUN']);

print("Exiting run - final number of iterations: $iterations, final duration of run: $duration\n");