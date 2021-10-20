#!/bin/bash

EtlBenchmark () {
  ./scripts/luth_analytics.sh etl "$1" "$2"
}

DelayBenchmark () {
  ./scripts/luth_analytics.sh delay "$1" "$2"
}

TwoGram () {
  ./scripts/luth_analytics.sh twogram "$1" "$2"
}


WordCount () {
  ./scripts/luth_analytics.sh wordcount "$1" "$2"
}



use_ropes_options=(true)


### WordCount Benchmark
for use_ropes in ${use_ropes_options[*]}; do   # The quotes are necessary here
  WordCount python "$use_ropes"
  WordCount js "$use_ropes"
  WordCount java "$use_ropes"
done

### TwoGam Benchmark
for use_ropes in ${use_ropes_options[*]}; do   # The quotes are necessary here
  TwoGram python "$use_ropes"
  TwoGram js "$use_ropes"
  TwoGram java "$use_ropes"
done



### Delay Benchmark
for use_ropes in ${use_ropes_options[*]}; do   # The quotes are necessary here
  DelayBenchmark python "$use_ropes"
  DelayBenchmark js "$use_ropes"
  DelayBenchmark java "$use_ropes"
done

### Etl Benchmark
for use_ropes in ${use_ropes_options[*]}; do   # The quotes are necessary here
  EtlBenchmark python "$use_ropes"
  EtlBenchmark js "$use_ropes"
  EtlBenchmark java "$use_ropes"
done
