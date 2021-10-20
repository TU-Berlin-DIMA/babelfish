#!/bin/bash

execute() {
  echo "Execute $1 with $2 and $3 and $4"
  ./scripts/luth_layout.sh "$1" "$2" "$3" "$4"
}

queries=("csv" "arrow" "row" "column")
languages=("rel")
settings=("true" "false")
fields=("1" "2" "3" "4" "5" "6" "7" "8" "9" "10" "11" "12" "13" "14" "15" "16")

for query in ${queries[*]}; do # The quotes are necessary here
  for language in ${languages[*]}; do # The quotes are necessary here
    for field in ${fields[*]}; do # The quotes are necessary here
      for setting in ${settings[*]}; do # The quotes are necessary here
         execute "$query" "$language" "$field" "$setting"
      done
    done
  done
done
