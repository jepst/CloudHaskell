module Main (main) where

import Remote.Process (standaloneLocalRegistry)

main = standaloneLocalRegistry "config"
