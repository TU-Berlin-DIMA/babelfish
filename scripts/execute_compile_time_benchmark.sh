#!/bin/bash

execute() {
  echo "Execute $1 with $2"
  ./scripts/luth_compile_time.sh  "$1" "$2"
}

queries=("q1" "q3" "q6" "q18")
languages=("rel" "python" "js" "java")

for query in ${queries[*]}; do # The quotes are necessary here
  for language in ${languages[*]}; do # The quotes are necessary here
    execute "$query" "$language"
  done
done
