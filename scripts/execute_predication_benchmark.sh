#!/bin/bash

execute() {
  echo "Execute $1 with $2"
  ./scripts/luth_predication.sh  "$1" "$2" "$3"
}

selecs=("1" "2" "3" "4" "5" "6" "7" "8" "9" "10" "20" "30" "40" "50" "60" "70" "80" "90" "91" "92" "93" "94" "95" "96" "97" "98" "99")
languages=("js" "java" "python" "rel")
pred=("true")

for sel in ${selecs[*]}; do # The quotes are necessary here
  for language in ${languages[*]}; do # The quotes are necessary here
    for pre in ${pred[*]}; do # The quotes are necessary here
      execute "$sel" "$language" "$pre"
    done
  done
done
