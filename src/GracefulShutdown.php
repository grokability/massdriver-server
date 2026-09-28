<?php

namespace Massdriver;

interface GracefulShutdown
{
    function graceful_shutdown(): void;
}