<?php

$files = [dirname(__DIR__).'/massdriver.php'];
foreach (['src', 'tests'] as $directory) {
    foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator(dirname(__DIR__).'/'.$directory)) as $file) {
        if ($file->getExtension() === 'php') {
            $files[] = $file->getPathname();
        }
    }
}
$failed = false;
foreach ($files as $file) {
    passthru(escapeshellarg(PHP_BINARY).' -l '.escapeshellarg($file), $status);
    $failed = $failed || $status !== 0;
}
exit($failed ? 1 : 0);
