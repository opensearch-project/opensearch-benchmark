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

import random
import unittest.mock as mock
from unittest import TestCase

from osbenchmark import exceptions
from osbenchmark.utils import repo


class BenchmarkRepositoryTests(TestCase):
    @mock.patch("osbenchmark.utils.io.exists", autospec=True)
    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    def test_fails_in_offline_mode_if_not_a_git_repo(self, is_working_copy, exists):
        is_working_copy.return_value = False
        exists.return_value = True

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            repo.BenchmarkRepository(
                default_directory=None,
                root_dir="/benchmark-resources",
                repo_name="unit-test",
                resource_name="unittest-resources",
                offline=True)

        self.assertEqual("[/benchmark-resources/unit-test] must be a git repository.\n\n"
                         "Please run:\ngit -C /benchmark-resources/unit-test init", ctx.exception.args[0])

    @mock.patch("osbenchmark.utils.io.exists", autospec=True)
    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    def test_does_nothing_in_offline_mode_if_not_existing(self, is_working_copy, exists):
        is_working_copy.return_value = False
        exists.return_value = False

        r = repo.BenchmarkRepository(
            default_directory=None,
            root_dir="/benchmark-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=True)

        self.assertFalse(r.remote)

    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    def test_does_nothing_if_working_copy_present(self, is_working_copy):
        is_working_copy.return_value = True

        r = repo.BenchmarkRepository(
                default_directory=None,
                root_dir="/benchmark-resources",
                repo_name="unit-test",
                resource_name="unittest-resources",
                offline=True)

        self.assertFalse(r.remote)

    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    @mock.patch("osbenchmark.utils.git.clone", autospec=True)
    def test_clones_initially(self, clone, is_working_copy):
        is_working_copy.return_value = False

        r = repo.BenchmarkRepository(
            default_directory="git@gitrepos.example.org/benchmark-resources",
            root_dir="/benchmark-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        self.assertTrue(r.remote)

        clone.assert_called_with(src="/benchmark-resources/unit-test", remote="git@gitrepos.example.org/benchmark-resources")

    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    @mock.patch("osbenchmark.utils.git.fetch", autospec=True)
    def test_fetches_if_already_cloned(self, fetch, is_working_copy):
        is_working_copy.return_value = True

        repo.BenchmarkRepository(
            default_directory="git@gitrepos.example.org/benchmark-resources",
            root_dir="/benchmark-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        fetch.assert_called_with(src="/benchmark-resources/unit-test")

    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    @mock.patch("osbenchmark.utils.git.fetch")
    def test_does_not_fetch_if_suppressed(self, fetch, is_working_copy):
        is_working_copy.return_value = True

        r = repo.BenchmarkRepository(
            default_directory="git@gitrepos.example.org/benchmark-resources",
            root_dir="/benchmark-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False,
            fetch=False)

        self.assertTrue(r.remote)

        self.assertEqual(0, fetch.call_count)

    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    @mock.patch("osbenchmark.utils.git.fetch")
    def test_ignores_fetch_errors(self, fetch, is_working_copy):
        fetch.side_effect = exceptions.SupplyError("Testing error")
        is_working_copy.return_value = True

        r = repo.BenchmarkRepository(
            default_directory="git@gitrepos.example.org/benchmark-resources",
            root_dir="/benchmark-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)
        # no exception during the call - we reach this here
        self.assertTrue(r.remote)

        fetch.assert_called_with(src="/benchmark-resources/unit-test")

    @mock.patch("osbenchmark.utils.git.head_revision")
    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    @mock.patch("osbenchmark.utils.git.fetch", autospec=True)
    @mock.patch("osbenchmark.utils.git.branches", autospec=True)
    @mock.patch("osbenchmark.utils.git.checkout", autospec=True)
    @mock.patch("osbenchmark.utils.git.rebase", autospec=True)
    def test_updates_from_remote(self, rebase, checkout, branches, fetch, is_working_copy, head_revision):
        branches.return_value = ["1", "2", "5", "main"]
        is_working_copy.return_value = True
        head_revision.return_value = "123a"

        r = repo.BenchmarkRepository(
            default_directory="git@gitrepos.example.org/benchmark-resources",
            root_dir="/benchmark-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=random.choice([True, False]))

        r.update(distribution_version="1.7.3", distribution_type="opensearch")

        branches.assert_called_with("/benchmark-resources/unit-test", remote=True)
        rebase.assert_called_with("/benchmark-resources/unit-test", branch="OS-1")
        checkout.assert_called_with("/benchmark-resources/unit-test", branch="OS-1")

    @mock.patch("osbenchmark.utils.git.head_revision")
    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    @mock.patch("osbenchmark.utils.git.fetch", autospec=True)
    @mock.patch("osbenchmark.utils.git.branches", autospec=True)
    @mock.patch("osbenchmark.utils.git.checkout", autospec=True)
    @mock.patch("osbenchmark.utils.git.rebase")
    @mock.patch("osbenchmark.utils.git.current_branch")
    def test_updates_locally(self, curr_branch, rebase, checkout, branches, fetch, is_working_copy, head_revision):
        curr_branch.return_value = "5"
        branches.return_value = ["1", "2", "5", "main"]
        is_working_copy.return_value = True
        head_revision.return_value = "123a"

        r = repo.BenchmarkRepository(
            default_directory=None,
            root_dir="/benchmark-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        r.update(distribution_version="6.0.0", distribution_type="")

        branches.assert_called_with("/benchmark-resources/unit-test", remote=False)
        self.assertEqual(0, rebase.call_count)
        checkout.assert_called_with("/benchmark-resources/unit-test", branch="main")

    @mock.patch("osbenchmark.utils.git.head_revision")
    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    @mock.patch("osbenchmark.utils.git.fetch", autospec=True)
    @mock.patch("osbenchmark.utils.git.tags", autospec=True)
    @mock.patch("osbenchmark.utils.git.branches", autospec=True)
    @mock.patch("osbenchmark.utils.git.checkout", autospec=True)
    @mock.patch("osbenchmark.utils.git.rebase")
    @mock.patch("osbenchmark.utils.git.current_branch")
    def test_fallback_to_tags(self, curr_branch, rebase, checkout, branches, tags, fetch, is_working_copy, head_revision):
        curr_branch.return_value = "main"
        branches.return_value = ["5", "main"]
        tags.return_value = ["v1", "v1.7", "v2"]
        is_working_copy.return_value = True
        head_revision.return_value = "123a"

        r = repo.BenchmarkRepository(
            default_directory=None,
            root_dir="/benchmark-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        r.update(distribution_version="1.7.4", distribution_type="opensearch")

        branches.assert_called_with("/benchmark-resources/unit-test", remote=False)
        self.assertEqual(0, rebase.call_count)
        tags.assert_called_with("/benchmark-resources/unit-test")
        checkout.assert_called_with("/benchmark-resources/unit-test", branch="v1.7")

    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    @mock.patch("osbenchmark.utils.git.fetch", autospec=True)
    @mock.patch("osbenchmark.utils.git.tags", autospec=True)
    @mock.patch("osbenchmark.utils.git.branches", autospec=True)
    @mock.patch("osbenchmark.utils.git.checkout")
    @mock.patch("osbenchmark.utils.git.rebase")
    def test_does_not_update_unknown_branch_remotely(self, rebase, checkout, branches, tags, fetch, is_working_copy):
        branches.return_value = ["1", "2", "5", "main"]
        tags.return_value = []
        is_working_copy.return_value = True

        r = repo.BenchmarkRepository(
            default_directory="git@gitrepos.example.org/benchmark-resources",
            root_dir="/benchmark-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        self.assertTrue(r.remote)

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            r.update(distribution_version="4.0.0")

        self.assertEqual("Cannot find unittest-resources for distribution version 4.0.0", ctx.exception.args[0])

        calls = [
            # first try to find it remotely...
            mock.call("/benchmark-resources/unit-test", remote=True),
            # ... then fallback to local
            mock.call("/benchmark-resources/unit-test", remote=False),
        ]

        branches.assert_has_calls(calls)
        tags.assert_called_with("/benchmark-resources/unit-test")
        self.assertEqual(0, checkout.call_count)
        self.assertEqual(0, rebase.call_count)

    @mock.patch("osbenchmark.utils.git.head_revision")
    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    @mock.patch("osbenchmark.utils.git.fetch", autospec=True)
    @mock.patch("osbenchmark.utils.git.tags", autospec=True)
    @mock.patch("osbenchmark.utils.git.branches", autospec=True)
    @mock.patch("osbenchmark.utils.git.checkout", autospec=True)
    @mock.patch("osbenchmark.utils.git.rebase")
    @mock.patch("osbenchmark.utils.git.current_branch")
    def test_does_not_update_unknown_branch_remotely_local_fallback(self, curr_branch, rebase, checkout, branches, tags,
                                                                    fetch, is_working_copy, head_revision):
        curr_branch.return_value = "main"
        # we have only "main" remotely but a few more branches locally
        branches.side_effect = ["5", ["1", "2", "5", "main"]]
        tags.return_value = []
        is_working_copy.return_value = True
        head_revision.retun_value = "123a"

        r = repo.BenchmarkRepository(
            default_directory="git@gitrepos.example.org/benchmark-resources",
            root_dir="/benchmark-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        r.update(distribution_version="1.7.3", distribution_type="opensearch")

        calls = [
            # first try to find it remotely...
            mock.call("/benchmark-resources/unit-test", remote=True),
            # ... then fallback to local
            mock.call("/benchmark-resources/unit-test", remote=False),
        ]

        branches.assert_has_calls(calls)
        self.assertEqual(0, tags.call_count)
        checkout.assert_called_with("/benchmark-resources/unit-test", branch="OS-1")
        self.assertEqual(0, rebase.call_count)

    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    @mock.patch("osbenchmark.utils.git.fetch", autospec=True)
    @mock.patch("osbenchmark.utils.git.tags", autospec=True)
    @mock.patch("osbenchmark.utils.git.branches", autospec=True)
    @mock.patch("osbenchmark.utils.git.checkout")
    @mock.patch("osbenchmark.utils.git.rebase")
    def test_does_not_update_unknown_branch_locally(self, rebase, checkout, branches, tags, fetch, is_working_copy):
        branches.return_value = ["1", "2", "5", "main"]
        tags.return_value = []
        is_working_copy.return_value = True

        r = repo.BenchmarkRepository(
            default_directory=None,
            root_dir="/benchmark-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            r.update(distribution_version="4.0.0")

        self.assertEqual("Cannot find unittest-resources for distribution version 4.0.0", ctx.exception.args[0])

        branches.assert_called_with("/benchmark-resources/unit-test", remote=False)
        self.assertEqual(0, checkout.call_count)
        self.assertEqual(0, rebase.call_count)

    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    @mock.patch("osbenchmark.utils.git.fetch", autospec=True)
    @mock.patch("osbenchmark.utils.git.checkout", autospec=True)
    def test_checkout_revision(self, checkout, fetch, is_working_copy):
        is_working_copy.return_value = True

        r = repo.BenchmarkRepository(
            default_directory=None,
            root_dir="/benchmark-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        r.checkout("abcdef123")

        checkout.assert_called_with("/benchmark-resources/unit-test", "abcdef123")
