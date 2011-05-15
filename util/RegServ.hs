module Main (main) where

-- This is the standalone node registry service.

import Remote.Process (standaloneLocalRegistry)

main = standaloneLocalRegistry "config"
