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

  # make it38, it39, etc. so they run as concurrent GHA jobs
  make "it${1//./}"
}

$@

