<?php

namespace Massdriver;

use Dotenv\Dotenv;
use Massdriver\ReactAws\ReactAws;
use React\EventLoop\LoopInterface;
use React\Promise\Promise;
use React\Stream\ReadableResourceStream;
use function React\Promise\all;

class FederatedClientCredentialsRefresher
{
    protected static array $credentials_array;
    protected ReactAws $sts_client;

    const array IMPORTANT_CREDENTIALS = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'AWS_SESSION_TOKEN',
        '_SESSION_EXPIRATION'
    ];


    public function __construct(
        protected LoopInterface $loop,
        public string $directory,
    ) {
        $this->sts_client = new ReactAws($this->loop,'Sts',[
            'version' => '2011-06-15',
        ]);

        $refresh_time = $this->reload_credentials_sync();
        $this->loop->addTimer()
        $this->loop->addSignal(SIGHUP,fn () => $this->reload_credentials_sync());
    }

    public function reload_credentials_sync(): float
    {
        $soonest_refresh = PHP_FLOAT_MAX;
        foreach(glob("/var/www/snipe-host/*/.env") as $env_file) {
            //maybe delegate this stuff to run()? Use this just to get the slug list
            $params = Dotenv::createArrayBacked($env_file)->load();
            $important_params = [];
            foreach($params as $env_var_name => $value) {
                if(in_array($env_var_name,self::IMPORTANT_CREDENTIALS)) {
                    $important_params[$env_var_name] = $value;
                    if($env_var_name == '_SESSION_EXPIRATION') {
                        if($value < $soonest_refresh) {
                            $soonest_refresh = $value;
                        }
                    }
                }
            }
            static::$credentials_array[$env_file] = $important_params;
        }
        return $soonest_refresh;
    }

    public function run()
    {
        //first, refresh everything that needs refreshing in a loop.
        $now = microtime(true);
        $soonest_expiration = PHP_FLOAT_MAX;
        $promises = [];
        foreach(static::$credentials_array as $slug => $credentials) {
            if($credentials['_SESSION_EXPIRATION'] <= $now-something) {
                $reader = new Promise($this->loop, function () use ($env_filename) {
                    $stream = new ReadableResourceStream($env_filename);
                    $stream->on('data',function ($chunk) {

                        //append data to...where?
                        //and maybe I can do the 'search' for the keys *right here* - though if a key got broken up
                        //due to a stream buffer, we might not catch it unless we're careful.
                        //I think the bit to note is that we want to grep *out* the sensitive vars
                    });
                    $stream->on('end', /* actually resolve myself? */);
                    $stream->on('error', /* reject myself? */);
                });
                $token_fetcher = $this->sts_client->getFederationTokenAsync([
                    'DurationSeconds' => A_LOnG_TIME,
                    'Name' => $slug,
                    'Policy' => $inline_policy,
                    'PolicyArns' => [
                        ['arn' => $string],
                        ['arn' => $string],
                    ]
                ])->catch($retry_with_timeout_and_possible_failure); //we *may* have this already? If so, then that's great!

                $promises[$slug] = all(['reader' => $reader,'token_fetcher' => $token_fetcher])->then(
                    function ($results) use (&$soonest_expiration) {
                        //the entire .env has been read into memory, *and* we have a new token.

                        //first, find the lines with the 'important bits' and yank those bits.
                        foreach(dfsdsf)

                        if($new_expiration < $soonest_expiration) {
                            $soonest_expiration = $new_expiration;
                        }
                    }
                );
            }
        }

        $outcomes = [];

        foreach ($promises as $key => $promise) {
            $outcomes[$key] = $promise->then(
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