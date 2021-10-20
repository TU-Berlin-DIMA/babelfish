#!/bin/bash

execute() {
  echo "Execute $1 with $2 and $3"
  ./scripts/luth_multi.sh "$1" "rel" "$2" "$3"
}

threads=("1" "2" "4" "6" "8" "10" "12" "24")
settings=("true" "false")

for thread in ${threads[*]}; do # The quotes are necessary here
  for setting in ${settings[*]}; do # The quotes are necessary here
    execute "$1" "$thread" "$setting"
  done
done
