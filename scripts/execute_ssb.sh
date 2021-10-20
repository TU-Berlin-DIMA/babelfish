#!/bin/bash

execute() {
  echo "Execute $1 with $2"
  ./scripts/luth.sh "$1" "$2"
}

queries=("ssb11" "ssb21" "ssb31" "ssb41")
languages=("rel" "python" "js" "java")

for query in ${queries[*]}; do # The quotes are necessary here
  for language in ${languages[*]}; do # The quotes are necessary here
    execute "$query" "$language"
  done
done
