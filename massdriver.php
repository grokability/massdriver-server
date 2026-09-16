<?php

require "vendor/autoload.php";

use Aws\Exception\AwsException;
use Aws\Sqs\SqsClient;

//we have to use 'unsafe' to actually set the environment variables,
// so that AWS will have access to them.
$dotenv = Dotenv\Dotenv::createUnsafeImmutable(__DIR__);
$dotenv->safeLoad();
$envget = fn ($name) => !$dotenv->required($name) ?: $_ENV[$name];

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

enum ProcessMode {
    case ExtendMessageVisibilityAndWait;
    case CalculateSoonestDeadline;
}

print("Starting Massdriver...".($dev_mode ? "IN DEV MODE": "")."\n");
if(!function_exists('pcntl_sigtimedwait')) {
    print("Shimming in terrible 'pcntl' solution for local development! DO NOT USE THIS IN PRODUCTION!\n");
    function pcntl_sigtimedwait($ignore1,$ignore2,$seconds,$microseconds = 0) {
        usleep((int)$seconds * 1000 + $microseconds);
        return true;
    }
}

function do_process_housekeeping()
{
    global $process_array,$client,$async_requests;
    //we loop through the $process_array *three times* - once to look for finished tasks, once to extend
    // the receiveMessage deadlines, and once to find out the soonest deadline that could pass.
    // We do the three passes because actual time *could* elapse
    //during the finished-tasks handling, which *could* change the numbers on the next wait.
    // TODO - I think I can re-unify the two loops for extensions and for calculating next deadlines?
    //        because I've pulled out the Guzzle settle/wait thing
    foreach ($process_array as $key => $process) {
        $process_status = proc_get_status($process['handle']);
        if (!$process_status['running']) {
            print("Finished executing command to run! status is: " . print_r($process_status, true) . "\n");
            $return_var = $process_status['exitcode']; //depending on the version of PHP, this *might* only be able to be fetched once?
            if ($return_var === 0) {
                unlink($process['stdout']);
                unlink($process['stderr']);
                $delete_results = $client->deleteMessageAsync([
                    'QueueUrl' => $_ENV['SQS_QUEUE'],
                    'ReceiptHandle' => $process['receipt_handle'],
                ]);

                print("Maybe we deleted something(asynchronously)? " . $delete_results->getState() . "\n");
                $async_requests [] = $delete_results;
            } else {
                print("Execution failed :/ - exit code: $return_var\n");
                print("STDOUT:\n");
                print("STDERR:\n");
            }
            proc_close($process['handle']);
            unset($process_array[$key]);
        }
    }

    //now, we need to figure out how long to wait
    //loop through all running processes and figure out when the next one
    //is going to have to get extended
    $soonest_deadline = 2 ** 31 - 1;
    $check_soonest = function ($x) use (&$soonest_deadline) {
        if ($x < $soonest_deadline) {
            $soonest_deadline = $x;
        }
    };

    /*
    //we have to run this loop *twice* - once to extend the deadlines, then we have to wait for Guzzle to finish,
    // and then once *again* to calculate the _new_ wait times, which might be different.
    /foreach ([ProcessMode::ExtendMessageVisibilityAndWait, ProcessMode::CalculateSoonestDeadline] as $mode) {
        $current_time = microtime(true);
        foreach ($process_array as &$process) {
            $elapsed_time = $current_time - $process['start_time'];

            //if more than half of the Visibility Window has expired, double it.
            if ($elapsed_time > $process['validity_window'] / 2) {
                $wait_threshold = $process['validity_window'];
                if($mode == ProcessMode::ExtendMessageVisibilityAndWait) {
                    print("Extending deadline - current validity window is " . ($process['validity_window'] / 2) . " and $elapsed_time seconds have elapsed. Extending to " . $process['validity_window'] . "\n");
                    $process['validity_window'] *= 2;
                    $async_requests[] = $client->changeMessageVisibilityAsync([
                        'VisibilityTimeout' => (int) $process['validity_window'],
                        'QueueUrl' => $_ENV['SQS_QUEUE'],
                        'ReceiptHandle' => $process['receipt_handle'],
                    ]);
                }
                if($mode == ProcessMode::CalculateSoonestDeadline) {
                    // the wait threshold was already doubled in the *previous* pass,
                    // so we need to wait for *half* of that
                    $check_soonest($wait_threshold/2); //the soonest *this* task could fire is `$wait_threshold`, which is half the validity window
                }
            } else {
                // whereas *this* process could fire once it gets halfway past its _own_ validity window.
                if($mode == ProcessMode::CalculateSoonestDeadline) {
                    $check_soonest($process['validity_window'] / 2 - $elapsed_time);
                }
            }
        }
        unset($process); //safety since that's a reference, and it *can* get carried-over and overwritten in some cases?
        if($mode == ProcessMode::ExtendMessageVisibilityAndWait) {
            print("NOT doing the guzzle-wait thing...");
            //now, we need to make sure that all of our async requests have finished -
            //GuzzleHttp\Promise\Utils::settle($async_requests)->wait();
            // since we're now doing this stuff *right before* we try to fetch SQS tasks, we can assume/guesstimate
            // that *that* will advance our async requests, so we're skipping it here for now.
            // $async_requests = [];
        }
    } */
    $current_time = microtime(true);
    foreach ($process_array as &$process) {
        $elapsed_time = $current_time - $process['start_time'];

        //if more than half of the Visibility Window has expired, double it.
        if ($elapsed_time > $process['validity_window'] / 2) {
            $wait_threshold = $process['validity_window'];
            print("Extending deadline - current validity window is " . ($process['validity_window'] / 2) . " and $elapsed_time seconds have elapsed. Extending to " . $process['validity_window'] . "\n");
            $process['validity_window'] *= 2;
            $async_requests[] = $client->changeMessageVisibilityAsync([
                'VisibilityTimeout' => (int) $process['validity_window'],
                'QueueUrl' => $_ENV['SQS_QUEUE'],
                'ReceiptHandle' => $process['receipt_handle'],
            ]);
            $check_soonest($wait_threshold); //the soonest *this* task could fire is `$wait_threshold`, which is half the validity window
        } else {
            // whereas *this* process could fire once it gets halfway past its _own_ validity window.
            $check_soonest($process['validity_window'] / 2 - $elapsed_time);
        }
    }
    return $soonest_deadline;
}

// BEGIN SETTING UP MAIN EVENT LOOP!
$start = microtime(true);
$iterations = 0;
$duration = null;
$number_of_empties = 0;
$oldMask = null;
$max_concurrency = $envget('MAX_CONCURRENCY');
$process_array = [];
$async_requests = [];

do {
    try {
        $deadline = do_process_housekeeping();
        $remaining_slots = $max_concurrency - count($process_array);
        $SAFETY_MARGIN = 2.0;
        $polltime = $deadline-$SAFETY_MARGIN;
        $poll_start_time = microtime(true);
        if ($remaining_slots == 0) {
            print("No slots remaining, not calling receiveMessage");
            GuzzleHttp\Promise\Utils::settle($async_requests)->wait(); //this handles the various time limit extensions for tasks and such
            $async_requests = [];
        } elseif($polltime < 0) {
            print("Calculated deadline $deadline is less than the safety margin ($SAFETY_MARGIN), not calling receiveMessage\n");
            // the 'wait' from the 'settle' will probably eat up *some* of the time - but what if it's empty?
            // Well, it shouldn't matter - the fcntl_sigtimedwait() will eat up the delay
            GuzzleHttp\Promise\Utils::settle($async_requests)->wait();
            $async_requests = [];
        } else {
            $clamped_polltime = (int) floor(min($envget('POLL_TIME'),$polltime));
            print (count($process_array) . " tasks running, going to try to fetch $remaining_slots to get to a total of $max_concurrency, waiting for $polltime seconds (Clamped to: $clamped_polltime).\n");
            $iterations++;
            $results = $client->receiveMessage([
                'AttributeNames' => ['SentTimestamp'],
                'MaxNumberOfMessages' => $remaining_slots,
                // 'MessageAttributeNames' => ['All'], // 'MessageGroupId' is the magic parameter!
                'MessageSystemAttributeNames' => ['All'],
                'QueueUrl' => $_ENV['SQS_QUEUE'],
                'WaitTimeSeconds' => $clamped_polltime,
                'VisibilityTimeout' => (int) $envget('MESSAGE_VISIBILITY_TIMEOUT'),
            ]);
            if (empty($results->get('Messages'))) {
                $number_of_empties++;
                if ($number_of_empties % 3 == 0) {
                    print($number_of_empties . " empty message(s) received.\n");
                }
            } else if($number_of_empties) {
                print($number_of_empties . " empty message(s) received before getting a non-empty result\n");
                $number_of_empties = 0;
            }
            // print("Result count is: ".$results->count()."\n");
            // print($results['Messages']);
            // *this* part adds jobs and forks them and tracks the proc_open handles
            foreach ($results->get('Messages') ?? [] as $message) {
                print_r($message);
                try {
                    $payload = json_decode($message['Body'], true, 8, JSON_THROW_ON_ERROR);

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
                        0 => ['file', '/dev/null', 'r'],
                        1 => ['file', $stdout, 'w'],
                        2 => ['file', $stderr, 'w'],
                    ];
                    $pipes = []; //we don't actually reference this anywhere; that's ok.
                    // TODO - split the $command_to_run into an array to skip an exec() of 'sh'
                    if ($oldMask) {
                        if (!pcntl_sigprocmask(SIG_SETMASK, $oldMask)) {
                            throw new RuntimeException("Unable to re-set original signals mask");
                        }
                    }
                    $exec_results = proc_open($command_to_run, $descriptor_spec, $pipes);
                    $start_time = microtime(true);
                    if (!$exec_results) {
                        throw new RuntimeException("Could not run command: $command_to_run");
                    }
                    if (!pcntl_sigprocmask(SIG_BLOCK, [SIGCHLD], $oldMask)) {
                        throw new RuntimeException('Cannot block SIGCHLD');
                    }
                    $process_array[] = [
                        'handle' => $exec_results,
                        'validity_window' => $_ENV['MESSAGE_VISIBILITY_TIMEOUT'],
                        'start_time' => $start_time,
                        'receipt_handle' => $message['ReceiptHandle'],
                        'stdout' => $stdout,
                        'stderr' => $stderr,
                    ];
                    // IMPORTANT: the previous $deadline could have been 2 billion seconds
                    // (with an empty process list, that is what's going to happen)
                    if($deadline > $_ENV['MESSAGE_VISIBILITY_TIMEOUT']/2) {
                        $deadline = $_ENV['MESSAGE_VISIBILITY_TIMEOUT']/2;
                    }
                } catch (\RuntimeException $e) {
                    print("RUNTIME exception caught: $e - not removing job.\n");
                } catch (\Exception|JsonException $e) {
                    // this would be things like: malformatted $payload, failed json_decode, missing 'command' element, and stuff like that
                    // we'll *never* be able to process those messages, so we just make them immediately visible so they will get rapidly
                    // eaten up and chucked into the DLQ
                    print("Exception caught! $e\n");
                    if (!$dev_mode) {
                        $async_requests[] = $client->changeMessageVisibilityAsync([
                            'QueueUrl' => $_ENV['SQS_QUEUE'],
                            'ReceiptHandle' => $message['ReceiptHandle'],
                            'VisibilityTimeout' => 0
                        ]);
                    }
                }
            }
        }
        // Now, the all-important 'pcntl_sigtimedwait' - this pretty much powers the entire event-loop
        if(count($process_array)>0) {
            $poll_actual_duration = $poll_start_time - microtime(true);
            $remaining_wait_time = $deadline - $poll_actual_duration;
            if($remaining_wait_time < 0) {
                $remaining_wait_time = 0;
            }
            if($remaining_wait_time > $_ENV['DURATION_TO_RUN']) {
                print_r($process_array);
                throw new RuntimeException("Can't wait for $remaining_wait_time seconds");
            }

            $info=[]; //we generally don't actually use this, FYI.
            $signal = pcntl_sigtimedwait([SIGCHLD], $info, $remaining_wait_time); //wait for $remaining_wait_time seconds, but wake up earlier if needed

            if ($signal === false || $signal === -1) {
                $error = pcntl_get_last_error();

                if ($error !== PCNTL_EAGAIN && $error !== PCNTL_EINTR) {
                    throw new RuntimeException(pcntl_strerror($error));
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
