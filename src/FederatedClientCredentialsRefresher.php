<?php

namespace Massdriver;

use Aws\Sts\StsClient;
use Dotenv\Dotenv;
use React\EventLoop\LoopInterface;
use React\Stream\ReadableResourceStream;
use function React\Promise\all;
use function React\Promise\resolve;

class FederatedClientCredentialsRefresher
{
    public static array $credentials_array;
    protected static StsClient $sts_client;

    public function __construct(
        public string $directory,
    ) {
        // SYNCHRONOUS constructor! Do not run in event loop
        foreach(glob("/var/www/snipe-host/*/.env") as $env_file) {
            $params = Dotenv::createArrayBacked($env_file)->load();
            $important_credentials = [
                'AWS_ACCESS_KEY_ID',
                'AWS_SECRET_ACCESS_KEY',
                'AWS_SESSION_TOKEN',
                '_SESSION_EXPIRATION'
            ];
            static::$credentials_array[$env_file] = array_intersect_something($important_credentials, $params);
        }
    }

    public function run(LoopInterface $loop)
    {
        //first, refresh everything that needs refreshing in a loop.
        $now = microtime(true);
        $soonest_expiration = PHP_FLOAT_MAX;
        $promises = [];
        foreach(static::$credentials_array as $slug => $credentials) {
            if($credentials['_SESSION_EXPIRATION'] <= $now-something) {
                $reader = new ReadableResourceStream($env_filename); //then...readmore...until eof?
                $token_fetcher = resolve(self::$sts_client->getFederationTokenAsync([

                ])->otherwise($retry_with_timeout_and_possible_failure); //we *may* have this already? If so, then that's great!

                $promises[$slug] = all(['reader' => $reader,'token_fetcher' => $token_fetcher])->then(function ($results) use ($loop,&$soonest_expiration) {
                    //the entire .env (probably) has been read into memory, and we have a new token.

                    //first, find the lines with the 'important bits' and yank those bits.

                    if($new_expiration < $soonest_expiration) {
                        $soonest_expiration = $new_expiration;
                    }
                });
            }
        }

        $outcomes = [];

        foreach ($promises as $key => $promise) {
            $outcomes[$key] = resolve($promise)->then(
                static fn ($value) => $value,
                static fn (\Throwable $reason) => $reason,
            );
        }

        all($outcomes)->then(function ($promise_outcomes) use ($loop, &$soonest_expiration) {
            foreach($promise_outcomes as $slug => $promise_outcome) {
                if($promise_outcome instanceof \Throwable) {
                    //rejected promise!
                    //one thing we *could* do, is to futureTick this method so we can get that missing STS token?
                    //though that would be an *immediate* retry, which we might want to back-off on.
                } else {
                    //fulfilled promise!
                    self::$credentials_array[$slug] = $promise_outcome; //I guess?
                }
            }

            //then, set a timer for the 'soonest' credential that needs refreshing.
            $loop->addTimer($soonest_expiration, fn () => $this->run($loop));
        });

    }
}