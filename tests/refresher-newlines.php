<?php
declare(strict_types=1);

require dirname(__DIR__).'/vendor/autoload.php';

use Amp\Future;
use Massdriver\FederatedClientCredentialsRefresher as Refresher;

\Amp\File\filesystem(new \Amp\File\Driver\BlockingFilesystemDriver());

// Bypass STS construction: this test only rewrites files with dummy credentials.
$refresher = (new ReflectionClass(Refresher::class))->newInstanceWithoutConstructor();
$directory = sys_get_temp_dir().'/massdriver-newlines-'.bin2hex(random_bytes(8));
mkdir($directory.'/tenant', 0700, true);
$refresher->directory = $directory;
$path = $directory.'/tenant/.env';
$credentials = [
    'AWS_ACCESS_KEY_ID' => 'test-key',
    'AWS_SECRET_ACCESS_KEY' => 'test-secret',
    'AWS_SESSION_TOKEN' => 'test-token',
    '_SESSION_EXPIRATION' => 2051222400,
];
$block = Refresher::ENV_FILE_COMMENT."\n";
foreach ($credentials as $key => $value) {
    $block .= "$key=\"$value\"\n";
}
$unrelated = "\n# keep this comment\n\nAPP_NAME=example\n\n";
$cases = [
    'empty' => ['', $block],
    'blank' => [str_repeat("\n", 40), $block],
    'credentials' => [$block, $block],
    'accumulated blanks' => [str_repeat("\n", 40).rtrim($block, "\n"), $block],
    'CRLF credentials' => [str_repeat("\r\n", 40).str_replace("\n", "\r\n", $block), $block],
    'other settings' => [$unrelated.$block, $unrelated.$block],
    'missing final newline' => ["APP_NAME=example", "APP_NAME=example\n".$block],
];

try {
    foreach ($cases as $name => [$contents, $expected]) {
        file_put_contents($path, $contents);
        for ($iteration = 0; $iteration < 5; $iteration++) {
            $refresher->write_one_credential('tenant', Future::complete($credentials), Future::complete(file_get_contents($path)));
            if (file_get_contents($path) !== $expected) {
                throw new RuntimeException("Unexpected newlines for $name on refresh $iteration");
            }
            if (file_exists($path.'.tmp')) {
                throw new RuntimeException('Temporary file remained after refresh');
            }
        }
    }
    echo "PASS refresher newlines\n";
} finally {
    if (file_exists($path.'.tmp')) {
        unlink($path.'.tmp');
    }
    if (file_exists($path)) {
        unlink($path);
    }
    rmdir($directory.'/tenant');
    rmdir($directory);
}
