# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import re

from osbenchmark import exceptions
from osbenchmark.utils import io, process

MIN_REQUIRED_MAJOR_VERSION = 2
VERSION_REGEX = r'.* ([0-9]+)\.([0-9]+)\..*'

def probed(f):
    def probe(src, *args, **kwargs):
        try:
            out, _, status = process.run_subprocess_with_out_and_err("git --version")
        except FileNotFoundError:
            status = 1
        if status != 0:
            raise exceptions.SystemSetupError("Error invoking 'git', please install (or re-install).")
        match = re.search(VERSION_REGEX, out)
        if not match or int(match.group(1)) < MIN_REQUIRED_MAJOR_VERSION:
            raise exceptions.SystemSetupError("OpenSearch Benchmark requires at least version 2 of git.  "
                                              f"You have {out}.  Please update git.")
        return f(src, *args, **kwargs)
    return probe


def is_working_copy(src):
    """
    Checks whether the given directory is a git working copy.
    :param src: A directory. May or may not exist.
    :return: True iff the given directory is a git working copy.
    """
    return os.path.exists(src) and os.path.exists(os.path.join(src, ".git"))


@probed
def clone(src, remote):
    io.ensure_dir(src)
    # Don't swallow subprocess output, user might need to enter credentials...
    if process.run_subprocess_with_logging("git clone %s %s" % (remote, io.escape_path(src))):
        raise exceptions.SupplyError("Could not clone from [%s] to [%s]" % (remote, src))


@probed
def fetch(src, remote="origin"):
    if process.run_subprocess_with_logging("git -C {0} fetch --prune --tags {1}".format(io.escape_path(src), remote)):
        raise exceptions.SupplyError("Could not fetch source tree from [%s]" % remote)


@probed
def checkout(src_dir, branch="main"):
    if process.run_subprocess_with_logging("git -C {0} checkout {1}".format(io.escape_path(src_dir), branch)):
        raise exceptions.SupplyError("Could not checkout [%s]. Do you have uncommitted changes?" % branch)


@probed
def rebase(src_dir, remote="origin", branch="main"):
    checkout(src_dir, branch)
    if process.run_subprocess_with_logging("git -C {0} rebase {1}/{2}".format(io.escape_path(src_dir), remote, branch)):
        raise exceptions.SupplyError("Could not rebase on branch [%s]" % branch)


@probed
def pull(src_dir, remote="origin", branch="main"):
    fetch(src_dir, remote)
    rebase(src_dir, remote, branch)


@probed
def pull_ts(src_dir, ts):
    fetch(src_dir)
    clean_src = io.escape_path(src_dir)
    revision = process.run_subprocess_with_output(
        "git -C {0} rev-list -n 1 --before=\"{1}\" --date=iso8601 origin/main".format(clean_src, ts))[0].strip()
    if process.run_subprocess_with_logging("git -C {0} checkout {1}".format(clean_src, revision)):
        raise exceptions.SupplyError("Could not checkout source tree for timestamped revision [%s]" % ts)


@probed
def pull_revision(src_dir, revision):
    fetch(src_dir)
    if process.run_subprocess_with_logging("git -C {0} checkout {1}".format(io.escape_path(src_dir), revision)):
        raise exceptions.SupplyError("Could not checkout source tree for revision [%s]" % revision)


@probed
def head_revision(src_dir):
    return process.run_subprocess_with_output("git -C {0} rev-parse --short HEAD".format(
        io.escape_path(src_dir)))[0].strip()


@probed
def current_branch(src_dir):
    return process.run_subprocess_with_output("git -C {0} rev-parse --abbrev-ref HEAD".format(
        io.escape_path(src_dir)))[0].strip()


@probed
def branches(src_dir, remote=True):
    clean_src = io.escape_path(src_dir)
    if remote:
        # Because compatability issues with Git 2.40.0+, updated --format='%(refname:short)' to --format='%(refname)'
        return _cleanup_remote_branch_names(process.run_subprocess_with_output(
                "git -C {src} for-each-ref refs/remotes/ --format='%(refname)'".format(src=clean_src)))
    else:
        return _cleanup_local_branch_names(
                process.run_subprocess_with_output(
                        "git -C {src} for-each-ref refs/heads/ --format='%(refname:short)'".format(src=clean_src)))


@probed
def tags(src_dir):
    return _cleanup_tag_names(process.run_subprocess_with_output("git -C {0} tag".format(io.escape_path(src_dir))))


def _cleanup_remote_branch_names(branch_names):
    return [(b[b.rindex("/") + 1:]).strip() for b in branch_names if not b.endswith("/HEAD")]


def _cleanup_local_branch_names(branch_names):
    return [b.strip() for b in branch_names if not b.endswith("HEAD")]


def _cleanup_tag_names(tag_names):
    return [t.strip() for t in tag_names]
