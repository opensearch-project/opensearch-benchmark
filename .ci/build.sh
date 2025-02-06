#!/usr/bin/env bash

function setup {
  export BENCHMARK_HOME=$GITHUB_WORKSPACE

  export THESPLOG_FILE=$BENCHMARK_HOME/.benchmark/logs/actor-system-internal.log
  export THESPLOG_FILE_MAXSIZE=204800			# default is 50 KiB
  export THESPLOG_THRESHOLD=INFO			# default log level is WARNING

  export TERM=dumb
  export LC_ALL=en_US.UTF-8

  # Init pyenv.
  PATH=$HOME/.pyenv/shims:$PATH:$HOME/.pyenv/bin

  # OpenSearch has different JDK requirements:
  # - Gradle builds need JDK 21 (after Apache Lucene 10 upgrade)
  # - OpenSearch runtime operations need JDK 17
  # Store the current JAVA_HOME (Java 21) for Gradle
  export GRADLE_JAVA_HOME=$JAVA_HOME
  
  # Set JAVA_HOME to Java 17 for OpenSearch
  if [ -n "$JAVA17_HOME" ]; then
    echo "Setting JAVA_HOME to Java 17 for OpenSearch"
    export JAVA_HOME=$JAVA17_HOME
    java -version
  else
    echo "WARNING: JAVA17_HOME is not set!"
  fi
}

function build_and_unit_test {
  setup

  set -e
  make develop
  make lint
  make test
}

function run_it {
  setup

  docker pull ubuntu/squid:latest

  # Temporarily switch to Java 21 for Gradle builds if needed
  if [ -n "$GRADLE_JAVA_HOME" ]; then
    OLD_JAVA_HOME=$JAVA_HOME
    export JAVA_HOME=$GRADLE_JAVA_HOME
    echo "Switched to Java 21 for Gradle build"
    java -version
  fi

  # Run the integration test
  make "it${1//./}"

  # Switch back to Java 17 if we changed it
  if [ -n "$OLD_JAVA_HOME" ]; then
    export JAVA_HOME=$OLD_JAVA_HOME
    echo "Switched back to Java 17"
    java -version
  fi
}

$@

