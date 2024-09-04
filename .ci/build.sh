#!/usr/bin/env bash

pyenv_init() {
  PATH=$HOME/.pyenv/shims:$PATH:$HOME/.pyenv/bin
}

function setup {
  export THESPLOG_FILE="${THESPLOG_FILE:-${BENCHMARK_HOME}/.benchmark/logs/actor-system-internal.log}"
  # this value is in bytes, the default is 50kB. We increase it to 200kiB.
  export THESPLOG_FILE_MAXSIZE=${THESPLOG_FILE_MAXSIZE:-204800}
  # adjust the default log level from WARNING
  export THESPLOG_THRESHOLD="INFO"

  pyenv_init
  export TERM=dumb
  export LC_ALL=en_US.UTF-8
}

function build {
  setup

  set -e
  make install-devel
  make lint
  make test
}

function build_it {
  setup

  export BENCHMARK_HOME="$GITHUB_WORKSPACE"

  docker pull ubuntu/squid:latest

  # make it38, it39, etc. so they run as concurrent GHA jobs
  make "it${1//./}"
}

$@

