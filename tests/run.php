<?php
declare(strict_types=1);

// Fresh PHP processes isolate static task state, timers, signals and deliberate
// failure paths. Every case runs even if an earlier one fails.
$cases = [];
foreach (['transport', 'deletion-retry', 'failed-jobs', 'visibility', 'shutdown', 'single-worker', 'visibility-race', 'shutdown-listener'] as $name) {
    $cases['integration/'.$name] = ['amp-integration.php', $name];
}
foreach (['mapping', 'write-success', 'write-failure', 'write-error-cleanup', 'multiline', 'load', 'load-error', 'backoff', 'retry-isolation', 'reschedule', 'shutdown-in-flight'] as $name) {
    $cases['refresher/'.$name] = ['refresher.php', $name];
}
$filter = $argv[1] ?? '';
if ($filter !== '') {
    $cases = array_filter($cases, static fn ($name) => str_contains($name, $filter), ARRAY_FILTER_USE_KEY);
    if ($cases === []) { fwrite(STDERR, "No test matches: $filter\n"); exit(2); }
}
$failed = 0;
foreach ($cases as $name => [$script, $case]) {
    $process = proc_open([PHP_BINARY, __DIR__.'/'.$script, $case], [
        0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w'],
    ], $pipes, dirname(__DIR__));
    if (!is_resource($process)) { throw new RuntimeException('Could not launch '.$name); }
    fclose($pipes[0]);
    stream_set_blocking($pipes[1], false);
    stream_set_blocking($pipes[2], false);
    $output = '';
    $deadline = microtime(true) + 30;
    $timedOut = false;
    do {
        $output .= stream_get_contents($pipes[1]).stream_get_contents($pipes[2]);
        $status = proc_get_status($process);
        if (!$status['running']) { break; }
        if (microtime(true) >= $deadline) {
            $timedOut = true;
            proc_terminate($process, 9);
            break;
        }
        usleep(10000);
    } while (true);
    $output .= stream_get_contents($pipes[1]).stream_get_contents($pipes[2]);
    fclose($pipes[1]);
    fclose($pipes[2]);
    $closed = proc_close($process);
    $code = $status['exitcode'] >= 0 ? $status['exitcode'] : $closed;
    if (!$timedOut && $code === 0) {
        echo "PASS $name\n";
    } else {
        $failed++;
        echo "FAIL $name".($timedOut ? ' (30-second deadline)' : '')."\n";
        echo trim($output)."\n\n";
    }
}
echo count($cases)." cases, $failed failures\n";
exit($failed === 0 ? 0 : 1);
