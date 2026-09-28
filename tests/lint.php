<?php

$files = [dirname(__DIR__).'/massdriver.php'];
foreach (['src', 'tests'] as $directory) {
    foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator(dirname(__DIR__).'/'.$directory)) as $file) {
        if ($file->getExtension() === 'php') {
            $files[] = $file->getPathname();
        }
    }
}
foreach ($files as $file) {
    passthru(escapeshellarg(PHP_BINARY).' -l '.escapeshellarg($file), $status);
    if ($status !== 0) { exit($status); }
}
