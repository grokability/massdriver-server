<?php

namespace Massdriver;

use Amp\Future;
use Dotenv\Dotenv;
use Revolt\EventLoop;
use function Amp\async;
use function Amp\File\isFile;
use function Amp\File\listFiles;
use function Amp\File\read;

/** Tenant credential loading. STS issuance was an unfinished prototype. */
class FederatedClientCredentialsRefresher
{
    protected static array $credentials_array = [];
    private string $signal;
    private ?Future $reload = null;

    const array IMPORTANT_CREDENTIALS = [
        'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN', '_SESSION_EXPIRATION',
    ];

    public function __construct(public string $directory)
    {
        $this->reload_credentials_sync();
        $this->signal = EventLoop::onSignal(SIGHUP, fn () => $this->reload_credentials_sync());
        EventLoop::unreference($this->signal);
    }

    /** Historical name retained; waits cooperatively using Amp filesystem workers. */
    public function reload_credentials_sync(): float
    {
        return $this->reload_credentials()->await();
    }

    public function reload_credentials(): Future
    {
        if ($this->reload !== null && !$this->reload->isComplete()) {
            return $this->reload;
        }
        return $this->reload = async(function (): float {
            $credentials = [];
            $soonest = PHP_FLOAT_MAX;
            foreach (listFiles($this->directory) as $tenant) {
                $filename = rtrim($this->directory, '/').'/'.$tenant.'/.env';
                if (!isFile($filename)) {
                    continue;
                }
                $params = array_intersect_key(Dotenv::parse(read($filename)), array_flip(self::IMPORTANT_CREDENTIALS));
                $credentials[$filename] = $params;
                if (isset($params['_SESSION_EXPIRATION'])) {
                    $expiration = is_numeric($params['_SESSION_EXPIRATION'])
                        ? (float) $params['_SESSION_EXPIRATION'] : strtotime($params['_SESSION_EXPIRATION']);
                    if ($expiration !== false) {
                        $soonest = min($soonest, $expiration);
                    }
                }
            }
            static::$credentials_array = $credentials;
            return $soonest;
        });
    }

    public function run()
    {
        throw new \LogicException('Federated credential issuance is not implemented: configure tenant STS policies and renewal before enabling it.');
    }

    public function close(): void
    {
        EventLoop::cancel($this->signal);
    }
}
