#!/bin/bash

execute() {
  echo "Execute $1 with $2 and $3"
  ./scripts/luth_multi.sh "$1" "rel" "$2" "$3"
}

groups=("1" "10" "100" "1000" "10000" "100000")
settings=("true" "false")

for group in ${groups[*]}; do # The quotes are necessary here
  for setting in ${settings[*]}; do # The quotes are necessary here
    rm table.lineitem_row_column.tmp
    execute "q6k" "$group" "$setting"
  done
done
