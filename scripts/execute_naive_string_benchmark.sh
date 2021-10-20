#!/bin/bash

EqualBenchmark () {
  ./scripts/luth_string_benchmark.sh equals_naive "$1" "$2"
}

CopyBenchmark () {
  ./scripts/luth_string_benchmark.sh copy_naive "$1" "$2"
}

LowercaseBenchmark () {
  ./scripts/luth_string_benchmark.sh lowercase_naive "$1" "$2"
}

UppercaseBenchmark () {
  ./scripts/luth_string_benchmark.sh uppercase_naive "$1" "$2"
}

SubstringBenchmark () {
  ./scripts/luth_string_benchmark.sh substring_naive "$1" "$2"
}
ReverseBenchmark () {
  ./scripts/luth_string_benchmark.sh reverse_naive "$1" "$2"
}
ConcatBenchmark () {
  ./scripts/luth_string_benchmark.sh concat_naive "$1" "$2"
}
UppercaseBenchmark () {
  ./scripts/luth_string_benchmark.sh uppercase_naive "$1" "$2"
}
SplitBenchmark () {
  ./scripts/luth_string_benchmark.sh split "$1" "$2"
}


use_ropes_options=(false)

### Equal Benchmark
for use_ropes in ${use_ropes_options[*]}; do   # The quotes are necessary here
  EqualBenchmark python "$use_ropes"
  EqualBenchmark js "$use_ropes"
  EqualBenchmark java "$use_ropes"
done

# Copy Benchmark
for use_ropes in "${use_ropes_options[@]}"; do   # The quotes are necessary here
  CopyBenchmark python "$use_ropes"
  CopyBenchmark js "$use_ropes"
  CopyBenchmark java "$use_ropes"
done

# Lowercase Benchmark
for use_ropes in "${use_ropes_options[@]}"; do   # The quotes are necessary here
  LowercaseBenchmark python "$use_ropes"
  LowercaseBenchmark js "$use_ropes"
  LowercaseBenchmark java "$use_ropes"
done

# Uppercase Benchmark
for use_ropes in "${use_ropes_options[@]}"; do   # The quotes are necessary here
  UppercaseBenchmark python "$use_ropes"
  UppercaseBenchmark js "$use_ropes"
  UppercaseBenchmark java "$use_ropes"
done

# Substring Benchmark
for use_ropes in "${use_ropes_options[@]}"; do   # The quotes are necessary here
  SubstringBenchmark python "$use_ropes"
  SubstringBenchmark js "$use_ropes"
  SubstringBenchmark java "$use_ropes"
done

# Reverse Benchmark
for use_ropes in "${use_ropes_options[@]}"; do   # The quotes are necessary here
  ReverseBenchmark python "$use_ropes"
  ReverseBenchmark js "$use_ropes"
  ReverseBenchmark java "$use_ropes"
done

# Concat Benchmark
for use_ropes in "${use_ropes_options[@]}"; do   # The quotes are necessary here
  ConcatBenchmark python "$use_ropes"
  ConcatBenchmark js "$use_ropes"
  ConcatBenchmark java "$use_ropes"
done

# Split Benchmark
for use_ropes in "${use_ropes_options[@]}"; do   # The quotes are necessary here
  SplitBenchmark python "$use_ropes"
  SplitBenchmark js "$use_ropes"
  SplitBenchmark java "$use_ropes"
 # CopyBenchmark rel "$use_ropes"
done