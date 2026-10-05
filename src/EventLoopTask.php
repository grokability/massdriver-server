<?php

namespace Massdriver;

abstract class EventLoopTask
{
    abstract public static  function get_env_vars(): array;

    public static function env_to_constructor_params(array $env): array
    {
        $results = [];
        foreach( static::get_env_vars() as $constructor_parameter => $environment_name ) {
            // skip integer keys; those have to get handled explicitly by the caller
            if(is_int($constructor_parameter)) {
                print "Skipping $environment_name\n";
                continue;
            }
            if(is_array($environment_name)) {
                $have_value = null;
                foreach($environment_name as $env_name => $transformation) {
                    if(isset($env[$env_name])) {
                        if($have_value) {
                            throw new \DomainException("Cannot have $have_value and $env_name both in `.env`");
                        }
                        $results[$constructor_parameter] = $transformation($env[$env_name]);
                        $have_value = $env_name;
                    }
                }
            } elseif(isset($env[$environment_name])) {
                $results[$constructor_parameter] = $env[$environment_name];
            }
        }
        return $results;
    }

    public static function get_env_var_names(): array
    {
        $results = [];
        foreach( static::get_env_vars() as $env_name ) {
            if(is_array($env_name)) {
                foreach(array_keys($env_name) as $env_subname) {
                    $results[] = $env_subname;
                }
            } else {
                $results[] = $env_name;
            }
        }
        return $results;
    }

    abstract function graceful_shutdown(): void;

    abstract public function __invoke():void;

    public function reload() {
        //do nothing by default
    }

    abstract public function get_iterations_count(): int;
}