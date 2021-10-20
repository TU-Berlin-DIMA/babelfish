#!/bin/bash

TyperPath=$1
DataPath=$2
SSBDataPath=$3
execute() {
  ./"$TyperPathrun_tpch"  "100" "$DataPath" "1" > "typer.out"
}

queries=("1h" "3h" "6h" "18h")

q=1h ./"$TyperPath/run_tpch" "100" "$DataPath" "1" >> "typer.out"
q=3h ./"$TyperPath/run_tpch" "100" "$DataPath" "1" >> "typer.out"
q=6h ./"$TyperPath/run_tpch" "100" "$DataPath" "1" >> "typer.out"
q=18h ./"$TyperPath/run_tpch" "100" "$DataPath" "1" >> "typer.out"

q=1.1h ./"$TyperPath/run_ssb" "100" "$SSBDataPath" "1" >> "typer.out"
q=2.1h ./"$TyperPath/run_ssb" "100" "$SSBDataPath" "1" >> "typer.out"
q=3.1h ./"$TyperPath/run_ssb" "100" "$SSBDataPath" "1" >> "typer.out"
q=4.1h ./"$TyperPath/run_ssb" "100" "$SSBDataPath" "1" >> "typer.out"