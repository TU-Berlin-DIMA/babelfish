#!/bin/bash

execute() {
  echo "Execute $1 with $2 and $3"
  ./scripts/luth_data.sh "$1" "$2" "$3"
}

queries=("q6_arrow" "q6_arrow" "q6_row" "q6_column")
languages=("rel")
settings=("true" "false")

for query in ${queries[*]}; do # The quotes are necessary here
  for language in ${languages[*]}; do # The quotes are necessary here
      for setting in ${settings[*]}; do # The quotes are necessary here
         execute "$query" "$language" "$setting"
      done
  done
done
