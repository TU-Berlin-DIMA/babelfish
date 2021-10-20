#!/bin/bash

truffleIterativePartialEscape=true
truffleInlining=true
truffleCompilation=true

languages=("python" "js" "java")

for language in ${languages[*]}; do # The quotes are necessary here

  # w/o all
  #./scripts/luth_compile.sh -noLoop -noPartialEscape "q6" "$language" "false" "false" "false" "false"
  # with compilation
  #./scripts/luth_compile.sh -noLoop -noPartialEscape "q6" "$language" "false" "false" "true" "false"
  # with inlining w/o partial escape
  ./scripts/luth_compile.sh -noLoop -noPartialEscape "q6" "$language" "true" "false" "true" "false"
  # with partial escape w/o inlining
 #./scripts/luth_compile.sh -noLoop "q6" "$language" "true" "true" "true" "false"
  # with partial escape with inlining with loop opt
  #./scripts/luth_compile.sh "q6" "$language" "true" "true" "true" "true"
  # w/o read
  #./scripts/luth_compile.sh -noRead "q6" "$language" "true" "true" "true" "true"
done
