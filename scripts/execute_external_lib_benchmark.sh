#!/bin/bash

execute() {
  echo "Execute $1 with $2"
  ./scripts/luth_external_lib.sh  "$1" "js"
}

queries=("cleaning" "time" "distance" "linearRegression" "blackSholes" "crimeIndex" "airlineEtl")

for q in ${queries[*]}; do # The quotes are necessary here
   execute "$q"
done
