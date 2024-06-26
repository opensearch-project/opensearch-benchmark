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
import random
import re
import textwrap
import unittest.mock as mock
import urllib.error
from unittest import TestCase

from osbenchmark import exceptions, config
from osbenchmark.workload import loader, workload
from osbenchmark.utils import io


def strip_ws(s):
    return re.sub(r"\s", "", s)


class StaticClock:
    NOW = 1453362707.0

    @staticmethod
    def now():
        return StaticClock.NOW

    @staticmethod
    def stop_watch():
        return None


class InstanceOf:
    "Tests whether an object belongs to a specified class."

    def __init__(self, cls):
        self.cls = cls

    def __eq__(self, other):
        return isinstance(other, self.cls)

    def __ne__(self, other):
        return not isinstance(other, self.cls)

    def __repr__(self):
        return f"<{self.cls.__name__} object at {hex(id(self))}>"


class SimpleWorkloadRepositoryTests(TestCase):
    @mock.patch("os.path.exists")
    @mock.patch("os.path.isdir")
    def test_workload_from_directory(self, is_dir, path_exists):
        is_dir.return_value = True
        path_exists.return_value = True

        repo = loader.SimpleWorkloadRepository("/path/to/workload/unit-test")
        self.assertEqual("unit-test", repo.workload_name)
        self.assertEqual(["unit-test"], repo.workload_names)
        self.assertEqual("/path/to/workload/unit-test", repo.workload_dir("unit-test"))
        self.assertEqual("/path/to/workload/unit-test/workload.json", repo.workload_file("unit-test"))

    @mock.patch("os.path.exists")
    @mock.patch("os.path.isdir")
    @mock.patch("os.path.isfile")
    def test_workload_from_file(self, is_file, is_dir, path_exists):
        is_file.return_value = True
        is_dir.return_value = False
        path_exists.return_value = True

        repo = loader.SimpleWorkloadRepository("/path/to/workload/unit-test/my-workload.json")
        self.assertEqual("my-workload", repo.workload_name)
        self.assertEqual(["my-workload"], repo.workload_names)
        self.assertEqual("/path/to/workload/unit-test", repo.workload_dir("my-workload"))
        self.assertEqual("/path/to/workload/unit-test/my-workload.json", repo.workload_file("my-workload"))

    @mock.patch("os.path.exists")
    @mock.patch("os.path.isdir")
    @mock.patch("os.path.isfile")
    def test_workload_from_named_pipe(self, is_file, is_dir, path_exists):
        is_file.return_value = False
        is_dir.return_value = False
        path_exists.return_value = True

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            loader.SimpleWorkloadRepository("a named pipe cannot point to a workload")
        self.assertEqual("a named pipe cannot point to a workload is neither a file nor a directory", ctx.exception.args[0])

    @mock.patch("os.path.exists")
    def test_workload_from_non_existing_path(self, path_exists):
        path_exists.return_value = False
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            loader.SimpleWorkloadRepository("/path/does/not/exist")
        self.assertEqual("Workload path /path/does/not/exist does not exist", ctx.exception.args[0])

    @mock.patch("os.path.isdir")
    @mock.patch("os.path.exists")
    def test_workload_from_directory_without_workload(self, path_exists, is_dir):
        # directory exists, but not the file
        path_exists.side_effect = [True, False]
        is_dir.return_value = True
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            loader.SimpleWorkloadRepository("/path/to/not/a/workload")
        self.assertEqual("Could not find workload.json in /path/to/not/a/workload", ctx.exception.args[0])

    @mock.patch("os.path.exists")
    @mock.patch("os.path.isdir")
    @mock.patch("os.path.isfile")
    def test_workload_from_file_but_not_json(self, is_file, is_dir, path_exists):
        is_file.return_value = True
        is_dir.return_value = False
        path_exists.return_value = True

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            loader.SimpleWorkloadRepository("/path/to/workload/unit-test/my-workload.xml")
        self.assertEqual("/path/to/workload/unit-test/my-workload.xml has to be a JSON file", ctx.exception.args[0])


class GitRepositoryTests(TestCase):
    class MockGitRepo:
        def __init__(self, remote_url, root_dir, repo_name, resource_name, offline, fetch=True):
            self.repo_dir = "%s/%s" % (root_dir, repo_name)

    @mock.patch("os.path.exists")
    @mock.patch("os.walk")
    def test_workload_from_existing_repo(self, walk, exists):
        walk.return_value = iter([(".", ["unittest", "unittest2", "unittest3"], [])])
        exists.return_value = True
        cfg = config.Config()
        cfg.add(config.Scope.application, "workload", "workload.name", "unittest")
        cfg.add(config.Scope.application, "workload", "repository.name", "default")
        cfg.add(config.Scope.application, "system", "offline.mode", False)
        cfg.add(config.Scope.application, "node", "root.dir", "/tmp")
        cfg.add(config.Scope.application, "benchmarks", "workload.repository.dir", "workloads")

        repo = loader.GitWorkloadRepository(cfg, fetch=False, update=False, repo_class=GitRepositoryTests.MockGitRepo)

        self.assertEqual("unittest", repo.workload_name)
        self.assertEqual(["unittest", "unittest2", "unittest3"], list(repo.workload_names))
        self.assertEqual("/tmp/workloads/default/unittest", repo.workload_dir("unittest"))
        self.assertEqual("/tmp/workloads/default/unittest/workload.json", repo.workload_file("unittest"))


class WorkloadPreparationTests(TestCase):
    @mock.patch("osbenchmark.utils.io.prepare_file_offset_table")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_does_nothing_if_document_file_available(self, is_file, get_size, prepare_file_offset_table):
        is_file.return_value = True
        get_size.return_value = 2000
        prepare_file_offset_table.return_value = 5

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        p.prepare_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                            document_file="docs.json",
                                                            document_archive="docs.json.bz2",
                                                            number_of_documents=5,
                                                            compressed_size_in_bytes=200,
                                                            uncompressed_size_in_bytes=2000),
                               data_root="/tmp")

        prepare_file_offset_table.assert_called_with("/tmp/docs.json", None, None, InstanceOf(loader.Downloader))

    @mock.patch("osbenchmark.utils.io.prepare_file_offset_table")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_decompresses_if_archive_available(self, is_file, get_size, prepare_file_offset_table):
        is_file.return_value = True
        get_size.return_value = 2000
        prepare_file_offset_table.return_value = 5

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        p.prepare_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                            document_file="docs.json",
                                                            document_archive="docs.json.bz2",
                                                            number_of_documents=5,
                                                            compressed_size_in_bytes=200,
                                                            uncompressed_size_in_bytes=2000),
                               data_root="/tmp")

        prepare_file_offset_table.assert_called_with("/tmp/docs.json", None, None, InstanceOf(loader.Downloader))

    @mock.patch("osbenchmark.utils.io.decompress")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_raise_error_on_wrong_uncompressed_file_size(self, is_file, get_size, decompress):
        # uncompressed file does not exist
        # compressed file exists
        # after decompression, uncompressed file exists
        is_file.side_effect = [False, True, True]
        # compressed file size is 200
        # uncompressed is corrupt, only 1 byte available
        get_size.side_effect = [200, 1]

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        with self.assertRaises(exceptions.DataError) as ctx:
            p.prepare_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                                document_file="docs.json",
                                                                document_archive="docs.json.bz2",
                                                                number_of_documents=5,
                                                                compressed_size_in_bytes=200,
                                                                uncompressed_size_in_bytes=2000),
                                   data_root="/tmp")
        self.assertEqual("[/tmp/docs.json] is corrupt. Extracted [1] bytes but [2000] bytes are expected.", ctx.exception.args[0])

        decompress.assert_called_with("/tmp/docs.json.bz2", "/tmp")

    @mock.patch("osbenchmark.utils.io.decompress")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_raise_error_if_compressed_does_not_contain_expected_document_file(self, is_file, get_size, decompress):
        # uncompressed file does not exist
        # compressed file exists
        # after decompression, uncompressed file does not exist (e.g. because the output file name is called differently)
        is_file.side_effect = [False, True, False]
        # compressed file size is 200
        get_size.return_value = 200

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        with self.assertRaises(exceptions.DataError) as ctx:
            p.prepare_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                                base_url="http://benchmarks.opensearch.org/corpora/unit-test",
                                                                document_file="docs.json",
                                                                document_archive="docs.json.bz2",
                                                                number_of_documents=5,
                                                                compressed_size_in_bytes=200,
                                                                uncompressed_size_in_bytes=2000),
                                   data_root="/tmp")
        self.assertEqual("Decompressing [/tmp/docs.json.bz2] did not create [/tmp/docs.json]. Please check with the workload author if the "
                         "compressed archive has been created correctly.", ctx.exception.args[0])

        decompress.assert_called_with("/tmp/docs.json.bz2", "/tmp")

    @mock.patch("osbenchmark.utils.io.prepare_file_offset_table")
    @mock.patch("osbenchmark.utils.io.decompress")
    @mock.patch("osbenchmark.utils.net.download")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_download_document_archive_if_no_file_available(self, is_file, get_size, ensure_dir, download, decompress,
                                                            prepare_file_offset_table):
        # uncompressed file does not exist
        # compressed file does not exist
        # after download compressed file exists
        # after download uncompressed file still does not exist (in main loop)
        # after download compressed file exists (in main loop)
        # after decompression, uncompressed file exists
        is_file.side_effect = [False, False, True, False, True, True, True, True]
        # compressed file size is 200 after download
        # compressed file size is 200 after download (in main loop)
        # uncompressed file size is 2000 after decompression
        # uncompressed file size is 2000 after decompression (in main loop)
        get_size.side_effect = [200, 200, 2000, 2000, None]

        prepare_file_offset_table.return_value = 5

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        p.prepare_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                            base_url="http://benchmarks.opensearch.org/corpora/unit-test",
                                                            document_file="docs.json",
                                                            document_archive="docs.json.bz2",
                                                            number_of_documents=5,
                                                            compressed_size_in_bytes=200,
                                                            uncompressed_size_in_bytes=2000),
                               data_root="/tmp")

        ensure_dir.assert_called_with("/tmp")
        decompress.assert_called_with("/tmp/docs.json.bz2", "/tmp")
        calls = [ mock.call("http://benchmarks.opensearch.org/corpora/unit-test/docs.json.bz2",
                            "/tmp/docs.json.bz2", 200, progress_indicator=mock.ANY) ]
        download.assert_has_calls(calls)
        prepare_file_offset_table.assert_called_with("/tmp/docs.json", 'http://benchmarks.opensearch.org/corpora/unit-test',
                                                     None, InstanceOf(loader.Downloader))

    @mock.patch("osbenchmark.utils.io.prepare_file_offset_table")
    @mock.patch("osbenchmark.utils.io.decompress")
    @mock.patch("osbenchmark.utils.net.download")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_download_document_archive_with_source_url_compressed(self, is_file, get_size, ensure_dir, download, decompress,
                                                                  prepare_file_offset_table):
        # uncompressed file does not exist
        # compressed file does not exist
        # after download compressed file exists
        # after download uncompressed file still does not exist (in main loop)
        # after download compressed file exists (in main loop)
        # after decompression, uncompressed file exists
        is_file.side_effect = [False, False, True, False, True, True, True, True]
        # compressed file size is 200 after download
        # compressed file size is 200 after download (in main loop)
        # uncompressed file size is 2000 after decompression
        # uncompressed file size is 2000 after decompression (in main loop)
        get_size.side_effect = [200, 200, 2000, 2000, None]

        prepare_file_offset_table.return_value = 5

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        p.prepare_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                            base_url="http://benchmarks.opensearch.org/corpora",
                                                    source_url="http://benchmarks.opensearch.org/corpora/unit-test/docs.json.bz2",
                                                            document_file="docs.json",
                                                            document_archive="docs.json.bz2",
                                                            number_of_documents=5,
                                                            compressed_size_in_bytes=200,
                                                            uncompressed_size_in_bytes=2000),
                               data_root="/tmp")

        ensure_dir.assert_called_with("/tmp")
        decompress.assert_called_with("/tmp/docs.json.bz2", "/tmp")
        download.assert_called_with("http://benchmarks.opensearch.org/corpora/unit-test/docs.json.bz2",
                                    "/tmp/docs.json.bz2", 200, progress_indicator=mock.ANY)
        prepare_file_offset_table.assert_called_with("/tmp/docs.json", 'http://benchmarks.opensearch.org/corpora',
                                                     'http://benchmarks.opensearch.org/corpora/unit-test/docs.json.bz2',
                                                     InstanceOf(loader.Downloader))

    @mock.patch("osbenchmark.utils.io.prepare_file_offset_table")
    @mock.patch("osbenchmark.utils.io.decompress")
    @mock.patch("osbenchmark.utils.net.download")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_download_document_with_source_url_uncompressed(self, is_file, get_size, ensure_dir, download, decompress,
                                                            prepare_file_offset_table):
        # uncompressed file does not exist
        # after download uncompressed file exists
        # after download uncompressed file exists (main loop)
        is_file.side_effect = [False, True, True]
        # uncompressed file size is 2000
        get_size.return_value = 2000
        scheme = random.choice(["http", "https", "s3", "gs"])

        prepare_file_offset_table.return_value = 5

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        p.prepare_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                    source_url=f"{scheme}://benchmarks.opensearch.org/corpora/unit-test/docs.json",
                                                            base_url=f"{scheme}://benchmarks.opensearch.org/corpora/",
                                                            document_file="docs.json",
                                                            # --> We don't provide a document archive here <--
                                                            document_archive=None,
                                                            number_of_documents=5,
                                                            compressed_size_in_bytes=200,
                                                            uncompressed_size_in_bytes=2000),
                               data_root="/tmp")

        ensure_dir.assert_called_with("/tmp")
        download.assert_called_with(f"{scheme}://benchmarks.opensearch.org/corpora/unit-test/docs.json",
                                    "/tmp/docs.json", 2000, progress_indicator=mock.ANY)
        prepare_file_offset_table.assert_called_with("/tmp/docs.json", f"{scheme}://benchmarks.opensearch.org/corpora/",
                                                     f"{scheme}://benchmarks.opensearch.org/corpora/unit-test/docs.json",
                                                     InstanceOf(loader.Downloader))

    @mock.patch("osbenchmark.utils.io.prepare_file_offset_table")
    @mock.patch("osbenchmark.utils.io.decompress")
    @mock.patch("osbenchmark.utils.net.download")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_download_document_with_trailing_baseurl_slash(self, is_file, get_size, ensure_dir, download, decompress,
                                                           prepare_file_offset_table):
        # uncompressed file does not exist
        # after download uncompressed file exists
        # after download uncompressed file exists (main loop)
        is_file.side_effect = [False, True, True, True]
        # uncompressed file size is 2000
        get_size.return_value = 2000
        scheme = random.choice(["http", "https", "s3", "gs"])

        prepare_file_offset_table.return_value = 5

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        p.prepare_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                            base_url=f"{scheme}://benchmarks.opensearch.org/corpora/unit-test/",
                                                            document_file="docs.json",
                                                            # --> We don't provide a document archive here <--
                                                            document_archive=None,
                                                            number_of_documents=5,
                                                            compressed_size_in_bytes=200,
                                                            uncompressed_size_in_bytes=2000),
                               data_root="/tmp")

        ensure_dir.assert_called_with("/tmp")
        calls = [ mock.call(f"{scheme}://benchmarks.opensearch.org/corpora/unit-test/docs.json", \
                            "/tmp/docs.json", 2000, progress_indicator=mock.ANY) ]
        download.assert_has_calls(calls)
        prepare_file_offset_table.assert_called_with("/tmp/docs.json", f"{scheme}://benchmarks.opensearch.org/corpora/unit-test/",
                                                     None, InstanceOf(loader.Downloader))

    @mock.patch("osbenchmark.utils.io.prepare_file_offset_table")
    @mock.patch("osbenchmark.utils.net.download")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_download_document_file_if_no_file_available(self, is_file, get_size, ensure_dir, download, prepare_file_offset_table):
        # uncompressed file does not exist
        # after download uncompressed file exists
        # after download uncompressed file exists (main loop)
        is_file.side_effect = [False, True, True, True]
        # uncompressed file size is 2000
        get_size.return_value = 2000

        prepare_file_offset_table.return_value = 5

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        p.prepare_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                            base_url="http://benchmarks.opensearch.org/corpora/unit-test",
                                                            document_file="docs.json",
                                                            # --> We don't provide a document archive here <--
                                                            document_archive=None,
                                                            number_of_documents=5,
                                                            compressed_size_in_bytes=200,
                                                            uncompressed_size_in_bytes=2000),
                               data_root="/tmp")

        ensure_dir.assert_called_with("/tmp")
        calls = [ mock.call("http://benchmarks.opensearch.org/corpora/unit-test/docs.json", \
                            "/tmp/docs.json", 2000, progress_indicator=mock.ANY) ]
        download.assert_has_calls(calls)
        prepare_file_offset_table.assert_called_with("/tmp/docs.json", 'http://benchmarks.opensearch.org/corpora/unit-test',
                                                     None, InstanceOf(loader.Downloader))

    @mock.patch("osbenchmark.utils.net.download")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("os.path.isfile")
    def test_raise_download_error_if_offline(self, is_file, ensure_dir, download):
        # uncompressed file does not exist
        is_file.return_value = False

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=True, test_mode=False),
                                         decompressor=loader.Decompressor())

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            p.prepare_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                                base_url="http://benchmarks.opensearch.org/corpora/unit-test",
                                                                document_file="docs.json",
                                                                number_of_documents=5,
                                                                uncompressed_size_in_bytes=2000),
                                   data_root="/tmp")

        self.assertEqual("Cannot find [/tmp/docs.json]. Please disable offline mode and retry.", ctx.exception.args[0])

        self.assertEqual(0, ensure_dir.call_count)
        self.assertEqual(0, download.call_count)

    @mock.patch("osbenchmark.utils.net.download")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("os.path.isfile")
    def test_raise_download_error_if_no_url_provided_and_file_missing(self, is_file, ensure_dir, download):
        # uncompressed file does not exist
        is_file.return_value = False

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        with self.assertRaises(exceptions.DataError) as ctx:
            p.prepare_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                                base_url=None,
                                                                document_file="docs.json",
                                                                document_archive=None,
                                                                number_of_documents=5,
                                                                uncompressed_size_in_bytes=2000),
                                   data_root="/tmp")

        self.assertEqual("Cannot download data because no base URL is provided.", ctx.exception.args[0])

        self.assertEqual(0, ensure_dir.call_count)
        self.assertEqual(0, download.call_count)

    @mock.patch("osbenchmark.utils.net.download")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_raise_download_error_if_no_url_provided_and_wrong_file_size(self, is_file, get_size, ensure_dir, download):
        # uncompressed file exists...
        is_file.return_value = True
        # but it's size is wrong
        get_size.return_value = 100

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        with self.assertRaises(exceptions.DataError) as ctx:
            p.prepare_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                                document_file="docs.json",
                                                                number_of_documents=5,
                                                                uncompressed_size_in_bytes=2000),
                                   data_root="/tmp")

        self.assertEqual("[/tmp/docs.json] is present but does not have the expected size of [2000] bytes and it "
                         "cannot be downloaded because no base URL is provided.", ctx.exception.args[0])

        self.assertEqual(0, ensure_dir.call_count)
        self.assertEqual(0, download.call_count)

    @mock.patch("osbenchmark.utils.net.download")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("os.path.isfile")
    def test_raise_download_error_no_test_mode_file(self, is_file, ensure_dir, download):
        # uncompressed file does not exist
        is_file.return_value = False

        download.side_effect = urllib.error.HTTPError("http://benchmarks.opensearch.org.s3.amazonaws.com/corpora/unit-test/docs-1k.json",
                                                      404, "", None, None)

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=True),
                                         decompressor=loader.Decompressor())

        with self.assertRaises(exceptions.DataError) as ctx:
            p.prepare_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                                base_url="http://benchmarks.opensearch.org/corpora/unit-test",
                                                                document_file="docs-1k.json",
                                                                number_of_documents=5,
                                                                uncompressed_size_in_bytes=None),
                                   data_root="/tmp")

        self.assertEqual("This workload does not support test mode. Ask the workload author to add it or disable "
                         "test mode and retry.", ctx.exception.args[0])

        ensure_dir.assert_called_with("/tmp")
        download.assert_called_with("http://benchmarks.opensearch.org/corpora/unit-test/docs-1k.json",
                                    "/tmp/docs-1k.json", None, progress_indicator=mock.ANY)

    @mock.patch("osbenchmark.utils.net.download")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("os.path.isfile")
    def test_raise_download_error_on_connection_problems(self, is_file, ensure_dir, download):
        # uncompressed file does not exist
        is_file.return_value = False

        download.side_effect = urllib.error.HTTPError("http://benchmarks.opensearch.org/corpora/unit-test/docs.json",
                                                      500, "Internal Server Error", None, None)

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        with self.assertRaises(exceptions.DataError) as ctx:
            p.prepare_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                                base_url="http://benchmarks.opensearch.org/corpora/unit-test",
                                                                document_file="docs.json",
                                                                number_of_documents=5,
                                                                uncompressed_size_in_bytes=2000),
                                   data_root="/tmp")

        self.assertEqual("Could not download [http://benchmarks.opensearch.org/corpora/unit-test/docs.json] "
                         "to [/tmp/docs.json] (HTTP status: 500, reason: Internal Server Error)", ctx.exception.args[0])

        ensure_dir.assert_called_with("/tmp")
        download.assert_called_with("http://benchmarks.opensearch.org/corpora/unit-test/docs.json",
                                    "/tmp/docs.json", 2000, progress_indicator=mock.ANY)

    @mock.patch("osbenchmark.utils.io.prepare_file_offset_table")
    @mock.patch("osbenchmark.utils.io.decompress")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_prepare_bundled_document_set_if_document_file_available(self, is_file, get_size, decompress, prepare_file_offset_table):
        is_file.return_value = True
        # check only uncompressed
        get_size.side_effect = [2000]
        prepare_file_offset_table.return_value = 5

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        self.assertTrue(p.prepare_bundled_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                                                    document_file="docs.json",
                                                                                    document_archive="docs.json.bz2",
                                                                                    number_of_documents=5,
                                                                                    compressed_size_in_bytes=200,
                                                                                    uncompressed_size_in_bytes=2000),
                                                       data_root="."))

        prepare_file_offset_table.assert_called_with("./docs.json", None, None, InstanceOf(loader.Downloader))

    @mock.patch("osbenchmark.utils.io.prepare_file_offset_table")
    @mock.patch("osbenchmark.utils.io.decompress")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_prepare_bundled_document_set_does_nothing_if_no_document_files(self, is_file, get_size, decompress, prepare_file_offset_table):
        # no files present
        is_file.return_value = False

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        self.assertFalse(p.prepare_bundled_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                                                     document_file="docs.json",
                                                                                     document_archive="docs.json.bz2",
                                                                                     number_of_documents=5,
                                                                                     compressed_size_in_bytes=200,
                                                                                     uncompressed_size_in_bytes=2000),
                                                        data_root="."))

        self.assertEqual(0, decompress.call_count)
        self.assertEqual(0, prepare_file_offset_table.call_count)

    def test_used_corpora(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [
                {"name": "logs-181998"},
                {"name": "logs-191998"},
                {"name": "logs-201998"},
            ],
            "corpora": [
                {
                    "name": "http_logs_unparsed",
                    "target-type": "type",
                    "documents": [
                        {
                            "target-index": "logs-181998",
                            "source-file": "documents-181998.unparsed.json.bz2",
                            "document-count": 2708746,
                            "compressed-bytes": 13064317,
                            "uncompressed-bytes": 303920342
                        },
                        {
                            "target-index": "logs-191998",
                            "source-file": "documents-191998.unparsed.json.bz2",
                            "document-count": 9697882,
                            "compressed-bytes": 47211781,
                            "uncompressed-bytes": 1088378738
                        },
                        {
                            "target-index": "logs-201998",
                            "source-file": "documents-201998.unparsed.json.bz2",
                            "document-count": 13053463,
                            "compressed-bytes": 63174979,
                            "uncompressed-bytes": 1456836090
                        }
                    ]
                },
                {
                    "name": "http_logs",
                    "target-type": "type",
                    "documents": [
                        {
                            "target-index": "logs-181998",
                            "source-file": "documents-181998.json.bz2",
                            "document-count": 2708746,
                            "compressed-bytes": 13815456,
                            "uncompressed-bytes": 363512754
                        },
                        {
                            "target-index": "logs-191998",
                            "source-file": "documents-191998.json.bz2",
                            "document-count": 9697882,
                            "compressed-bytes": 49439633,
                            "uncompressed-bytes": 1301732149
                        },
                        {
                            "target-index": "logs-201998",
                            "source-file": "documents-201998.json.bz2",
                            "document-count": 13053463,
                            "compressed-bytes": 65623436,
                            "uncompressed-bytes": 1744012279
                        }
                    ]
                }
            ],
            "operations": [
                {
                    "name": "bulk-index-1",
                    "operation-type": "bulk",
                    "corpora": ["http_logs"],
                    "indices": ["logs-181998"],
                    "bulk-size": 500
                },
                {
                    "name": "bulk-index-2",
                    "operation-type": "bulk",
                    "corpora": ["http_logs"],
                    "indices": ["logs-191998"],
                    "bulk-size": 500
                },
                {
                    "name": "bulk-index-3",
                    "operation-type": "bulk",
                    "corpora": ["http_logs_unparsed"],
                    "indices": ["logs-201998"],
                    "bulk-size": 500
                },
                {
                    "name": "node-stats",
                    "operation-type": "node-stats"
                },
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "schedule": [
                        {
                            "parallel": {
                                "tasks": [
                                    {
                                        "name": "index-1",
                                        "operation": "bulk-index-1",
                                    },
                                    {
                                        "name": "index-2",
                                        "operation": "bulk-index-2",
                                    },
                                    {
                                        "name": "index-3",
                                        "operation": "bulk-index-3",
                                    },
                                ]
                            }
                        },
                        {
                            "operation": "node-stats"
                        }
                    ]
                }
            ]
        }
        reader = loader.WorkloadSpecificationReader(selected_test_procedure="default-test_procedure")
        full_workload = reader("unittest", workload_specification, "/mappings")
        used_corpora = sorted(loader.used_corpora(full_workload), key=lambda c: c.name)
        self.assertEqual(2, len(used_corpora))
        self.assertEqual("http_logs", used_corpora[0].name)
        # each bulk operation requires a different data file but they should have been merged properly.
        self.assertEqual({"documents-181998.json.bz2", "documents-191998.json.bz2"},
                         {d.document_archive for d in used_corpora[0].documents})

        self.assertEqual("http_logs_unparsed", used_corpora[1].name)
        self.assertEqual({"documents-201998.unparsed.json.bz2"}, {d.document_archive for d in used_corpora[1].documents})

    @mock.patch("osbenchmark.utils.io.prepare_file_offset_table")
    @mock.patch("osbenchmark.utils.io.decompress")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_prepare_bundled_document_set_decompresses_compressed_docs(self, is_file, get_size, decompress, prepare_file_offset_table):
        # uncompressed is missing
        # decompressed is present
        # check if uncompressed is present after decompression
        # final loop iteration - uncompressed is present now
        is_file.side_effect = [False, True, True, True]
        # compressed
        # uncompressed after decompression
        # uncompressed in final loop iteration
        get_size.side_effect = [200, 2000, 2000]
        prepare_file_offset_table.return_value = 5

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        self.assertTrue(p.prepare_bundled_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                                                    document_file="docs.json",
                                                                                    document_archive="docs.json.bz2",
                                                                                    number_of_documents=5,
                                                                                    compressed_size_in_bytes=200,
                                                                                    uncompressed_size_in_bytes=2000),
                                                       data_root="."))

        prepare_file_offset_table.assert_called_with("./docs.json", None, None, InstanceOf(loader.Downloader))

    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_prepare_bundled_document_set_error_compressed_docs_wrong_size(self, is_file, get_size):
        # uncompressed is missing
        # decompressed is present
        is_file.side_effect = [False, True]
        # compressed has wrong size
        get_size.side_effect = [150]

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        with self.assertRaises(exceptions.DataError) as ctx:
            p.prepare_bundled_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                                        document_file="docs.json",
                                                                        document_archive="docs.json.bz2",
                                                                        number_of_documents=5,
                                                                        compressed_size_in_bytes=200,
                                                                        uncompressed_size_in_bytes=2000),
                                           data_root=".")

        self.assertEqual("[./docs.json.bz2] is present but does not have the expected size of [200] bytes.",
                         ctx.exception.args[0])

    @mock.patch("osbenchmark.utils.io.prepare_file_offset_table")
    @mock.patch("osbenchmark.utils.io.decompress")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_prepare_bundled_document_set_uncompressed_docs_wrong_size(self, is_file, get_size, decompress, prepare_file_offset_table):
        # uncompressed is present
        is_file.side_effect = [True]
        # uncompressed
        get_size.side_effect = [1500]

        p = loader.DocumentSetPreparator(workload_name="unit-test",
                                         downloader=loader.Downloader(offline=False, test_mode=False),
                                         decompressor=loader.Decompressor())

        with self.assertRaises(exceptions.DataError) as ctx:
            p.prepare_bundled_document_set(document_set=workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                                        document_file="docs.json",
                                                                        document_archive="docs.json.bz2",
                                                                        number_of_documents=5,
                                                                        compressed_size_in_bytes=200,
                                                                        uncompressed_size_in_bytes=2000),
                                           data_root=".")
        self.assertEqual("[./docs.json] is present but does not have the expected size of [2000] bytes.",
                         ctx.exception.args[0])

        self.assertEqual(0, prepare_file_offset_table.call_count)


class TemplateSource(TestCase):
    @mock.patch("osbenchmark.utils.io.dirname")
    @mock.patch.object(loader.TemplateSource, "read_glob_files")
    def test_entrypoint_of_replace_includes(self, patched_read_glob, patched_dirname):
        workload = textwrap.dedent("""
        {% import "benchmark.helpers" as benchmark with context %}
        {
          "version": 2,
          "description": "unittest workload",
          "data-url": "http://benchmarks.opensearch.org.s3.amazonaws.com/corpora/geonames",
          "indices": [
            {
              "name": "geonames",
              "body": "index.json"
            }
          ],
          "corpora": [
            {
              "name": "geonames",
              "base-url": "http://benchmarks.opensearch.org.s3.amazonaws.com/corpora/geonames",
              "documents": [
                {
                  "source-file": "documents-2.json.bz2",
                  "document-count": 11396505,
                  "compressed-bytes": 264698741,
                  "uncompressed-bytes": 3547614383
                }
              ]
            }
          ],
          "operations": [
            {{ benchmark.collect(parts="operations/*.json") }}
          ],
          "test_procedures": [
            {{ benchmark.collect(parts="test_procedures/*.json") }}
          ]
        }
        """)

        def dummy_read_glob(c):
            return "{{\"replaced {}\": \"true\"}}".format(c)

        patched_read_glob.side_effect = dummy_read_glob

        base_path = "~/.benchmark/benchmarks/workloads/default/geonames"
        template_file_name = "workload.json"
        tmpl_src = loader.TemplateSource(base_path, template_file_name)
        # pylint: disable=trailing-whitespace
        expected_response = textwrap.dedent("""
            {% import "benchmark.helpers" as benchmark with context %}
            {
              "version": 2,
              "description": "unittest workload",
              "data-url": "http://benchmarks.opensearch.org.s3.amazonaws.com/corpora/geonames",
              "indices": [
                {
                  "name": "geonames",
                  "body": "index.json"
                }
              ],
              "corpora": [
                {
                  "name": "geonames",
                  "base-url": "http://benchmarks.opensearch.org.s3.amazonaws.com/corpora/geonames",
                  "documents": [
                    {
                      "source-file": "documents-2.json.bz2",
                      "document-count": 11396505,
                      "compressed-bytes": 264698741,
                      "uncompressed-bytes": 3547614383
                    }
                  ]
                }
              ],
              "operations": [
                {"replaced ~/.benchmark/benchmarks/workloads/default/geonames/operations/*.json": "true"}
              ],
              "test_procedures": [
                {"replaced ~/.benchmark/benchmarks/workloads/default/geonames/test_procedures/*.json": "true"}
              ]
            }
            """)

        self.assertEqual(
            expected_response,
            tmpl_src.replace_includes(base_path, workload)
        )

    def test_read_glob_files(self):
        tmpl_obj = loader.TemplateSource(
            base_path="/some/path/to/a/benchmark/workload",
            template_file_name="workload.json",
            fileglobber=lambda pat: [
                os.path.join(os.path.dirname(__file__), "resources", "workload_fragment_1.json"),
                os.path.join(os.path.dirname(__file__), "resources", "workload_fragment_2.json")
            ]
        )
        response = tmpl_obj.read_glob_files("*workload_fragment_*.json")
        expected_response = '{\n  "item1": "value1"\n}\n,\n{\n  "item2": "value2"\n}\n'

        self.assertEqual(expected_response, response)


class TemplateRenderTests(TestCase):
    unittest_template_internal_vars = loader.default_internal_template_vars(clock=StaticClock)

    def test_render_simple_template(self):
        template = """
        {
            "key": {{'01-01-2000' | days_ago(now)}},
            "key2": "static value"
        }
        """

        rendered = loader.render_template(template, template_internal_vars=TemplateRenderTests.unittest_template_internal_vars)

        expected = """
        {
            "key": 5864,
            "key2": "static value"
        }
        """
        self.assertEqual(expected, rendered)

    def test_render_template_with_external_variables(self):
        template = """
        {
            "greeting": "{{greeting | default("Aloha")}}",
            "name": "{{name | default("stranger")}}"
        }
        """

        rendered = loader.render_template(template, template_vars={"greeting": "Hi"},
                                          template_internal_vars=TemplateRenderTests.unittest_template_internal_vars)

        expected = """
        {
            "greeting": "Hi",
            "name": "stranger"
        }
        """
        self.assertEqual(expected, rendered)

    def test_render_template_with_globbing(self):
        def key_globber(e):
            if e == "dynamic-key-*":
                return [
                    "dynamic-key-1",
                    "dynamic-key-2",
                    "dynamic-key-3",
                ]
            else:
                return []

        template = """
        {% import "benchmark.helpers" as benchmark %}
        {
            "key1": "static value",
            {{ benchmark.collect(parts="dynamic-key-*") }}

        }
        """

        source = io.DictStringFileSourceFactory({
            "dynamic-key-1": [
                textwrap.dedent('"dkey1": "value1"')
            ],
            "dynamic-key-2": [
                textwrap.dedent('"dkey2": "value2"')
            ],
            "dynamic-key-3": [
                textwrap.dedent('"dkey3": "value3"')
            ]
        })

        template_source = loader.TemplateSource("", "workload.json", source=source, fileglobber=key_globber)
        template_source.load_template_from_string(template)

        rendered = loader.render_template(
            template_source.assembled_source,
            template_internal_vars=TemplateRenderTests.unittest_template_internal_vars)

        expected = """
        {
            "key1": "static value",
            "dkey1": "value1",
            "dkey2": "value2",
            "dkey3": "value3"

        }
        """
        self.assertEqualIgnoreWhitespace(expected, rendered)

    def test_render_template_with_variables(self):
        template = """
        {% set _clients = clients if clients is defined else 16 %}
        {% set _bulk_size = bulk_size if bulk_size is defined else 100 %}
        {% import "benchmark.helpers" as benchmark with context %}
        {
            "key1": "static value",
            "dkey1": {{ _clients }},
            "dkey2": {{ _bulk_size }}
        }
        """
        rendered = loader.render_template(
            template,
            template_vars={"clients": 8},
            template_internal_vars=TemplateRenderTests.unittest_template_internal_vars)

        expected = """
        {
            "key1": "static value",
            "dkey1": 8,
            "dkey2": 100
        }
        """
        self.assertEqualIgnoreWhitespace(expected, rendered)

    def assertEqualIgnoreWhitespace(self, expected, actual):
        self.assertEqual(strip_ws(expected), strip_ws(actual))


class CompleteWorkloadParamsTests(TestCase):
    assembled_source = textwrap.dedent("""{% import "benchmark.helpers" as benchmark with context %}
        "key1": "value1",
        "key2": {{ value2 | default(3) }},
        "key3": {{ value3 | default("default_value3") }}
        "key4": {{ value2 | default(3) }}
    """)

    def test_check_complete_workload_params_contains_all_workload_params(self):
        complete_workload_params = loader.CompleteWorkloadParams()
        loader.register_all_params_in_workload(CompleteWorkloadParamsTests.assembled_source, complete_workload_params)

        self.assertEqual(
            ["value2", "value3"],
            complete_workload_params.sorted_workload_defined_params
        )

    def test_check_complete_workload_params_does_not_fail_with_no_workload_params(self):
        complete_workload_params = loader.CompleteWorkloadParams()
        loader.register_all_params_in_workload('{}', complete_workload_params)

        self.assertEqual(
            [],
            complete_workload_params.sorted_workload_defined_params
        )

    def test_unused_user_defined_workload_params(self):
        workload_params = {
            "number_of_repliacs": 1,  # deliberate typo
            "enable_source": True,  # unknown parameter
            "number_of_shards": 5
        }

        complete_workload_params = loader.CompleteWorkloadParams(user_specified_workload_params=workload_params)
        complete_workload_params.populate_workload_defined_params(list_of_workload_params=[
            "bulk_indexing_clients",
            "bulk_indexing_iterations",
            "bulk_size",
            "cluster_health",
            "number_of_replicas",
            "number_of_shards"]
        )

        self.assertEqual(
            ["enable_source", "number_of_repliacs"],
            sorted(complete_workload_params.unused_user_defined_workload_params())
        )

    def test_unused_user_defined_workload_params_doesnt_fail_with_detaults(self):
        complete_workload_params = loader.CompleteWorkloadParams()
        complete_workload_params.populate_workload_defined_params(list_of_workload_params=[
            "bulk_indexing_clients",
            "bulk_indexing_iterations",
            "bulk_size",
            "cluster_health",
            "number_of_replicas",
            "number_of_shards"]
        )

        self.assertEqual(
            [],
            sorted(complete_workload_params.unused_user_defined_workload_params())
        )


class WorkloadPostProcessingTests(TestCase):
    workload_with_params_as_string = textwrap.dedent("""{
        "indices": [
            {
                "name": "test-index",
                "body": "test-index-body.json",
                "types": ["test-type"]
            }
        ],
        "corpora": [
            {
                "name": "unittest",
                "documents": [
                    {
                        "source-file": "documents.json.bz2",
                        "document-count": 10,
                        "compressed-bytes": 100,
                        "uncompressed-bytes": 10000
                    }
                ]
            }
        ],
        "operations": [
            {
                "name": "index-append",
                "operation-type": "bulk",
                "bulk-size": 5000
            },
            {
                "name": "search",
                "operation-type": "search"
            }
        ],
        "test_procedures": [
            {
                "name": "default-test_procedure",
                "description": "Default test_procedure",
                "schedule": [
                    {
                        "clients": {{ bulk_indexing_clients | default(8) }},
                        "operation": "index-append",
                        "warmup-time-period": 100,
                        "time-period": 240
                    },
                    {
                        "parallel": {
                            "tasks": [
                                {
                                    "name": "search #1",
                                    "clients": 4,
                                    "operation": "search",
                                    "warmup-iterations": 1000,
                                    "iterations": 2000,
                                    "target-interval": 30
                                },
                                {
                                    "name": "search #2",
                                    "clients": 1,
                                    "operation": "search",
                                    "warmup-iterations": 1000,
                                    "iterations": 2000,
                                    "target-throughput": 200
                                },
                                {
                                    "name": "search #3",
                                    "clients": 1,
                                    "operation": "search",
                                    "iterations": 1
                                }
                            ]
                        }
                    }
                ]
            }
        ]
    }""")

    def test_post_processes_workload_spec(self):
        workload_specification = {
            "indices": [
                {
                    "name": "test-index",
                    "body": "test-index-body.json",
                    "types": ["test-type"]
                }
            ],
            "corpora": [
                {
                    "name": "unittest",
                    "documents": [
                        {
                            "source-file": "documents.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000
                        }
                    ]
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                    "bulk-size": 5000
                },
                {
                    "name": "search",
                    "operation-type": "search"
                }
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "description": "Default test_procedure",
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "warmup-time-period": 100,
                            "time-period": 240,
                        },
                        {
                            "parallel": {
                                "tasks": [
                                    {
                                        "name": "search #1",
                                        "clients": 4,
                                        "operation": "search",
                                        "warmup-iterations": 1000,
                                        "iterations": 2000,
                                        "target-interval": 30
                                    },
                                    {
                                        "name": "search #2",
                                        "clients": 1,
                                        "operation": "search",
                                        "warmup-iterations": 1000,
                                        "iterations": 2000,
                                        "target-throughput": 200
                                    },
                                    {
                                        "name": "search #3",
                                        "clients": 1,
                                        "operation": "search",
                                        "iterations": 1
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }

        expected_post_processed = {
            "indices": [
                {
                    "name": "test-index",
                    "body": "test-index-body.json",
                    "types": ["test-type"]
                }
            ],
            "corpora": [
                {
                    "name": "unittest",
                    "documents": [
                        {
                            "source-file": "documents-1k.json.bz2",
                            "document-count": 1000
                        }
                    ]
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                    "bulk-size": 5000
                },
                {
                    "name": "search",
                    "operation-type": "search"
                }
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "description": "Default test_procedure",
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "warmup-time-period": 0,
                            "time-period": 10,
                        },
                        {
                            "parallel": {
                                "tasks": [
                                    {
                                        "name": "search #1",
                                        "clients": 4,
                                        "operation": "search",
                                        "warmup-iterations": 4,
                                        "iterations": 4
                                    },
                                    {
                                        "name": "search #2",
                                        "clients": 1,
                                        "operation": "search",
                                        "warmup-iterations": 1,
                                        "iterations": 1
                                    },
                                    {
                                        "name": "search #3",
                                        "clients": 1,
                                        "operation": "search",
                                        "iterations": 1
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }

        complete_workload_params = loader.CompleteWorkloadParams()
        index_body = '{"settings": {"index.number_of_shards": {{ number_of_shards | default(5) }}, '\
                     '"index.number_of_replicas": {{ number_of_replicas | default(0)}} }}'

        cfg = config.Config()
        cfg.add(config.Scope.application, "workload", "test.mode.enabled", True)

        self.assertEqual(
            self.as_workload(expected_post_processed, complete_workload_params=complete_workload_params, index_body=index_body),
            loader.TestModeWorkloadProcessor(cfg).on_after_load_workload(
                self.as_workload(workload_specification, complete_workload_params=complete_workload_params, index_body=index_body)
            )
        )

        self.assertEqual(
            ["number_of_replicas", "number_of_shards"],
            complete_workload_params.sorted_workload_defined_params
        )

    def as_workload(self, workload_specification, workload_params=None, complete_workload_params=None, index_body=None):
        reader = loader.WorkloadSpecificationReader(
            workload_params=workload_params,
            complete_workload_params=complete_workload_params,
            source=io.DictStringFileSourceFactory({
                "/mappings/test-index-body.json": [index_body]
            })
        )
        return reader("unittest", workload_specification, "/mappings")


class WorkloadPathTests(TestCase):
    @mock.patch("os.path.exists")
    def test_sets_absolute_path(self, path_exists):
        path_exists.return_value = True

        cfg = config.Config()
        cfg.add(config.Scope.application, "benchmarks", "local.dataset.cache", "/data")

        default_test_procedure = workload.TestProcedure("default", default=True, schedule=[
            workload.Task(name="index", operation=workload.Operation("index", operation_type=workload.OperationType.Bulk), clients=4)
        ])
        another_test_procedure = workload.TestProcedure("other", default=False)
        t = workload.Workload(name="u", test_procedures=[another_test_procedure, default_test_procedure],
                        corpora=[
                            workload.DocumentCorpus("unittest", documents=[
                                workload.Documents(source_format=workload.Documents.SOURCE_FORMAT_BULK,
                                                document_file="docs/documents.json",
                                                document_archive="docs/documents.json.bz2")
                            ])
                        ],
                        indices=[workload.Index(name="test", types=["docs"])])

        loader.set_absolute_data_path(cfg, t)

        self.assertEqual("/data/unittest/docs/documents.json", t.corpora[0].documents[0].document_file)
        self.assertEqual("/data/unittest/docs/documents.json.bz2", t.corpora[0].documents[0].document_archive)


class WorkloadFilterTests(TestCase):
    def filter(self, workload_specification, include_tasks=None, exclude_tasks=None):
        cfg = config.Config()
        cfg.add(config.Scope.application, "workload", "include.tasks", include_tasks)
        cfg.add(config.Scope.application, "workload", "exclude.tasks", exclude_tasks)

        processor = loader.TaskFilterWorkloadProcessor(cfg)
        return processor.on_after_load_workload(workload_specification)

    def test_rejects_invalid_syntax(self):
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            self.filter(workload_specification=None, include_tasks=["valid", "a:b:c"])
        self.assertEqual("Invalid format for filtered tasks: [a:b:c]", ctx.exception.args[0])

    def test_rejects_unknown_filter_type(self):
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            self.filter(workload_specification=None, include_tasks=["valid", "op-type:index"])
        self.assertEqual("Invalid format for filtered tasks: [op-type:index]. Expected [type] but got [op-type].",
                         ctx.exception.args[0])

    def test_filters_tasks(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "create-index",
                    "operation-type": "create-index"
                },
                {
                    "name": "bulk-index",
                    "operation-type": "bulk"
                },
                {
                    "name": "node-stats",
                    "operation-type": "node-stats"
                },
                {
                    "name": "cluster-stats",
                    "operation-type": "custom-operation-type"
                },
                {
                    "name": "match-all",
                    "operation-type": "search",
                    "body": {
                        "query": {
                            "match_all": {}
                        }
                    }
                },
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "schedule": [
                        {
                            "operation": "create-index"
                        },
                        {
                            "parallel": {
                                "tasks": [
                                    {
                                        "name": "index-1",
                                        "operation": "bulk-index",
                                    },
                                    {
                                        "name": "index-2",
                                        "operation": "bulk-index",
                                    },
                                    {
                                        "name": "index-3",
                                        "operation": "bulk-index",
                                    },
                                    {
                                        "name": "match-all-parallel",
                                        "operation": "match-all",
                                    },
                                ]
                            }
                        },
                        {
                            "operation": "node-stats"
                        },
                        {
                            "name": "match-all-serial",
                            "operation": "match-all"
                        },
                        {
                            "operation": "cluster-stats"
                        },
                        {
                            "parallel": {
                                "tasks": [
                                    {
                                        "name": "query-filtered",
                                        "tags": "include-me",
                                        "operation": "match-all",
                                    },
                                    {
                                        "name": "index-4",
                                        "tags": ["include-me", "bulk-task"],
                                        "operation": "bulk-index",
                                    },
                                    {
                                        "name": "index-5",
                                        "operation": "bulk-index",
                                    }
                                ]
                            }
                        },
                        {
                            "name": "final-cluster-stats",
                            "operation": "cluster-stats",
                            "tags": "include-me"
                        }
                    ]
                }
            ]
        }
        reader = loader.WorkloadSpecificationReader()
        full_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual(7, len(full_workload.test_procedures[0].schedule))

        filtered = self.filter(full_workload, include_tasks=["index-3",
                                                          "type:search",
                                                          # Filtering should also work for non-core operation types.
                                                          "type:custom-operation-type",
                                                          "tag:include-me"])

        schedule = filtered.test_procedures[0].schedule
        self.assertEqual(5, len(schedule))
        self.assertEqual(["index-3", "match-all-parallel"], [t.name for t in schedule[0].tasks])
        self.assertEqual("match-all-serial", schedule[1].name)
        self.assertEqual("cluster-stats", schedule[2].name)
        self.assertEqual(["query-filtered", "index-4"], [t.name for t in schedule[3].tasks])
        self.assertEqual("final-cluster-stats", schedule[4].name)

    def test_filters_exclude_tasks(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "create-index",
                    "operation-type": "create-index"
                },
                {
                    "name": "bulk-index",
                    "operation-type": "bulk"
                },
                {
                    "name": "node-stats",
                    "operation-type": "node-stats"
                },
                {
                    "name": "cluster-stats",
                    "operation-type": "custom-operation-type"
                },
                {
                    "name": "match-all",
                    "operation-type": "search",
                    "body": {
                        "query": {
                            "match_all": {}
                        }
                    }
                },
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "schedule": [
                        {
                            "operation": "create-index"
                        },
                        {
                            "parallel": {
                                "tasks": [
                                    {
                                        "name": "index-1",
                                        "operation": "bulk-index",
                                    },
                                    {
                                        "name": "index-2",
                                        "operation": "bulk-index",
                                    },
                                    {
                                        "name": "index-3",
                                        "operation": "bulk-index",
                                    },
                                    {
                                        "name": "match-all-parallel",
                                        "operation": "match-all",
                                    },
                                ]
                            }
                        },
                        {
                            "operation": "node-stats"
                        },
                        {
                            "name": "match-all-serial",
                            "operation": "match-all"
                        },
                        {
                            "operation": "cluster-stats"
                        }
                    ]
                }
            ]
        }
        reader = loader.WorkloadSpecificationReader()
        full_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual(5, len(full_workload.test_procedures[0].schedule))

        filtered = self.filter(full_workload, exclude_tasks=["index-3", "type:search", "create-index"])

        schedule = filtered.test_procedures[0].schedule
        self.assertEqual(3, len(schedule))
        self.assertEqual(["index-1", "index-2"], [t.name for t in schedule[0].tasks])
        self.assertEqual("node-stats", schedule[1].name)
        self.assertEqual("cluster-stats", schedule[2].name)

    def test_unmatched_exclude_runs_everything(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "create-index",
                    "operation-type": "create-index"
                },
                {
                    "name": "bulk-index",
                    "operation-type": "bulk"
                },
                {
                    "name": "node-stats",
                    "operation-type": "node-stats"
                },
                {
                    "name": "cluster-stats",
                    "operation-type": "custom-operation-type"
                },
                {
                    "name": "match-all",
                    "operation-type": "search",
                    "body": {
                        "query": {
                            "match_all": {}
                        }
                    }
                },
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "schedule": [
                        {
                            "operation": "create-index"
                        },
                        {
                            "operation": "bulk-index"
                        },
                        {
                            "operation": "node-stats"
                        },
                        {
                            "name": "match-all-serial",
                            "operation": "match-all"
                        },
                        {
                            "operation": "cluster-stats"
                        }
                    ]
                }
            ]
        }

        reader = loader.WorkloadSpecificationReader()
        full_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual(5, len(full_workload.test_procedures[0].schedule))

        expected_schedule = full_workload.test_procedures[0].schedule.copy()
        filtered = self.filter(full_workload, exclude_tasks=["nothing"])

        schedule = filtered.test_procedures[0].schedule
        self.assertEqual(expected_schedule, schedule)

    def test_unmatched_include_runs_nothing(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "create-index",
                    "operation-type": "create-index"
                },
                {
                    "name": "bulk-index",
                    "operation-type": "bulk"
                },
                {
                    "name": "node-stats",
                    "operation-type": "node-stats"
                },
                {
                    "name": "cluster-stats",
                    "operation-type": "custom-operation-type"
                },
                {
                    "name": "match-all",
                    "operation-type": "search",
                    "body": {
                        "query": {
                            "match_all": {}
                        }
                    }
                },
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "schedule": [
                        {
                            "operation": "create-index"
                        },
                        {
                            "operation": "bulk-index"
                        },
                        {
                            "operation": "node-stats"
                        },
                        {
                            "name": "match-all-serial",
                            "operation": "match-all"
                        },
                        {
                            "operation": "cluster-stats"
                        }
                    ]
                }
            ]
        }

        reader = loader.WorkloadSpecificationReader()
        full_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual(5, len(full_workload.test_procedures[0].schedule))

        expected_schedule = []
        filtered = self.filter(full_workload, include_tasks=["nothing"])

        schedule = filtered.test_procedures[0].schedule
        self.assertEqual(expected_schedule, schedule)

class WorkloadRandomizationTests(TestCase):

    # Helper class used to set up queries with mock standard values for testing
    # We want >1 op to ensure logic for giving different ops their own lambdas is working properly
    class StandardValueHelper:
        def __init__(self):
            self.op_name_1 = "op-name-1"
            self.op_name_2 = "op-name-2"
            self.field_name_1 = "dummy_field_1"
            self.field_name_2 = "dummy_field_2"
            self.index_name = "dummy_index"

            # Make the saved standard values different from the functions generating the new values,
            # to be able to distinguish when we generate a new value vs draw an "existing" one.
            # in actual usage, these would come from the same function with some randomness in it
            self.saved_values = {
                self.op_name_1:{
                    self.field_name_1:{"lte":40, "gte":30},
                    self.field_name_2:{"lte":"06/06/2016", "gte":"05/05/2016", "format":"dd/MM/yyyy"}
                },
                self.op_name_2:{
                    self.field_name_1:{"lte":11, "gte":10}
                }
            }

            # Used to generate new values, in the source function
            self.new_values = {
                self.op_name_1:{
                    self.field_name_1:{"lte":41, "gte":31},
                    self.field_name_2:{"lte":"04/04/2016", "gte":"03/03/2016", "format":"dd/MM/yyyy"}
                },
                self.op_name_2:{
                    self.field_name_1:{"lte":15, "gte":14},
                }
            }

            self.op_1_query = {
                "name": self.op_name_1,
                "operation-type": "search",
                "body": {
                    "size": 0,
                    "query": {
                        "bool": {
                            "filter": {
                                "range": {
                                    self.field_name_1: {
                                        "lt": 50,
                                        "gte": 0
                                    }
                                },
                                "must": [
                                    {
                                        "range": {
                                            self.field_name_2: {
                                                "gte": "01/01/2015",
                                                "lte": "21/01/2015",
                                                "format": "dd/MM/yyyy"
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            }

            self.op_2_query = {
                "name": self.op_name_2,
                "operation-type": "search",
                "body": {
                    "size": 0,
                    "query": {
                        "range": {
                            self.field_name_1: {
                                "lt": 50,
                                "gte": 0
                            }
                        }
                    }
                }
            }

        def get_simple_workload(self):
            # Modified from test_filters_tasks
            workload_specification = {
                "description": "description for unit test",
                "indices": [{"name": self.index_name, "auto-managed": False}],
                "operations": [
                    {
                        "name": "create-index",
                        "operation-type": "create-index"
                    },
                    self.op_1_query,
                    self.op_2_query
                ],
                "test_procedures": [
                    {
                        "name": "default-test_procedure",
                        "schedule": [
                            {
                                "operation": "create-index"
                            },
                            {
                                "name": "dummy-task-name-1",
                                "operation": self.op_name_1,
                            },
                            {
                                "name": "dummy-task-name-2",
                                "operation": self.op_name_2,
                            },
                        ]
                    }
                ]
            }
            reader = loader.WorkloadSpecificationReader()
            full_workload = reader("unittest", workload_specification, "/mappings")
            return full_workload

        def get_standard_value_source(self, op_name, field_name):
            # Passed to the processor, to be able to find the standard value sources for all ops/fields.
            # The actual source functions for the op/field pairs, which in a real application
            # would be defined in the workload's workload.py and involve some randomization
            return lambda: self.new_values[op_name][field_name]

        def get_standard_value(self, op_name, field_name, index):
            # Passed to the processor, to be able to retrive the saved standard values for all ops/fields.
            return self.saved_values[op_name][field_name]

    def test_range_finding_function(self):
        cfg = config.Config()
        processor = loader.QueryRandomizerWorkloadProcessor(cfg)
        single_range_query = {
            "name": "distance_amount_agg",
            "operation-type": "search",
            "body": {
                "size": 0,
                "query": {
                "bool": {
                    "filter": {
                    "range": {
                        "trip_distance": {
                        "lt": 50,
                        "gte": 0
                        }
                    }
                    }
                }
                }
            }
        }
        single_range_query_result = processor.extract_fields_and_paths(single_range_query)
        single_range_query_expected = [("trip_distance", ["bool", "filter", "range"])]
        self.assertEqual(single_range_query_result, single_range_query_expected)

        multiple_nested_range_query = {
            "name": "date_histogram_agg",
            "operation-type": "search",
            "body": {
                "size": 0,
                "query": {
                    "range": {
                        "dropoff_datetime": {
                            "gte": "01/01/2015",
                            "lte": "21/01/2015",
                            "format": "dd/MM/yyyy"
                        }
                },
                "bool": {
                    "filter": {
                        "range": {
                            "dummy_field": {
                                "lte": 50,
                                "gt": 0
                            }
                        }
                    },
                    "must": [
                        {
                            "range": {
                                "dummy_field_2": {
                                    "gte": "1998-05-01T00:00:00Z",
                                    "lt": "1998-05-02T00:00:00Z"
                                }
                            }
                        },
                        {
                            "match": {
                            "status": "400"
                            }
                        },
                        {
                            "range": {
                                "dummy_field_3": {
                                    "gt": 10,
                                    "lt": 11
                                }
                            }
                        }
                    ]
                }
                }
            }
        }
        multiple_nested_range_query_result = processor.extract_fields_and_paths(multiple_nested_range_query)
        print("Multi result: ", multiple_nested_range_query_result)
        multiple_nested_range_query_expected = [
            ("dropoff_datetime", ["range"]),
            ("dummy_field", ["bool", "filter", "range"]),
            ("dummy_field_2", ["bool", "must", 0, "range"]),
            ("dummy_field_3", ["bool", "must", 2, "range"])
            ]
        self.assertEqual(multiple_nested_range_query_result, multiple_nested_range_query_expected)

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            params = {"body":{"contents":["not_a_valid_query"]}}
            _ = processor.extract_fields_and_paths(params)
            self.assertEqual(
                f"Cannot extract range query fields from these params: {params}\n, missing params[\"body\"][\"query\"]\n"
                f"Make sure the operation in operations/default.json is well-formed",
                         ctx.exception.args[0])

    def test_get_randomized_values(self):
        helper = self.StandardValueHelper()

        for rf, expected_values_dict in zip([1.0, 0.0], [helper.saved_values, helper.new_values]):
            # first test where we always draw a saved value, not a new random one
            # next test where we always draw a new random value. We've made them distinct, to be able to tell which codepath is taken
            cfg = config.Config()
            cfg.add(config.Scope.application, "workload", "randomization.repeat_frequency", rf)
            processor = loader.QueryRandomizerWorkloadProcessor(cfg)
            self.assertAlmostEqual(processor.rf, rf)

            # Test resulting params for operation 1
            workload = helper.get_simple_workload()
            modified_params = processor.get_randomized_values(workload, helper.op_1_query, op_name=helper.op_name_1,
                                                            get_standard_value=helper.get_standard_value,
                                                            get_standard_value_source=helper.get_standard_value_source)
            modified_range_1 = modified_params["body"]["query"]["bool"]["filter"]["range"][helper.field_name_1]
            modified_range_2 = modified_params["body"]["query"]["bool"]["filter"]["must"][0]["range"][helper.field_name_2]
            self.assertEqual(modified_range_1["lt"], expected_values_dict[helper.op_name_1][helper.field_name_1]["lte"])
            # Note it should keep whichever of lt/lte it found in the original query
            self.assertEqual(modified_range_1["gte"], expected_values_dict[helper.op_name_1][helper.field_name_1]["gte"])

            self.assertEqual(modified_range_2["lte"], expected_values_dict[helper.op_name_1][helper.field_name_2]["lte"])
            self.assertEqual(modified_range_2["gte"], expected_values_dict[helper.op_name_1][helper.field_name_2]["gte"])
            self.assertEqual(modified_range_2["format"], expected_values_dict[helper.op_name_1][helper.field_name_2]["format"])

            self.assertEqual(modified_params["index"], helper.index_name)

            # Test resulting params for operation 2
            workload = helper.get_simple_workload()
            modified_params = processor.get_randomized_values(workload, helper.op_2_query, op_name=helper.op_name_2,
                                                            get_standard_value=helper.get_standard_value,
                                                            get_standard_value_source=helper.get_standard_value_source)
            modified_range_1 = modified_params["body"]["query"]["range"][helper.field_name_1]

            self.assertEqual(modified_range_1["lt"], expected_values_dict[helper.op_name_2][helper.field_name_1]["lte"])
            self.assertEqual(modified_range_1["gte"], expected_values_dict[helper.op_name_2][helper.field_name_1]["gte"])
            self.assertEqual(modified_params["index"], helper.index_name)


    def test_on_after_load_workload(self):
        cfg = config.Config()
        processor = loader.QueryRandomizerWorkloadProcessor(cfg)
        # Do nothing with default config as randomization.enabled is false
        helper = self.StandardValueHelper()
        input_workload = helper.get_simple_workload()
        self.assertEqual(
            repr(input_workload),
            repr(processor.on_after_load_workload(input_workload, get_standard_value=helper.get_standard_value,
                                                            get_standard_value_source=helper.get_standard_value_source)))
        # It seems that comparing the workloads directly will incorrectly call them equal, even if they have differences,
        # so compare their string representations instead

        cfg = config.Config()
        cfg.add(config.Scope.application, "workload", "randomization.enabled", True)
        processor = loader.QueryRandomizerWorkloadProcessor(cfg)
        self.assertEqual(processor.randomization_enabled, True)
        self.assertEqual(processor.N, loader.QueryRandomizerWorkloadProcessor.DEFAULT_N)
        self.assertEqual(type(processor.N), int)
        self.assertEqual(processor.rf, loader.QueryRandomizerWorkloadProcessor.DEFAULT_RF)
        self.assertEqual(type(processor.rf), float)
        input_workload = helper.get_simple_workload()
        self.assertNotEqual(
            repr(input_workload),
            repr(processor.on_after_load_workload(input_workload, get_standard_value=helper.get_standard_value,
                                                            get_standard_value_source=helper.get_standard_value_source)))
        for test_procedure in input_workload.test_procedures:
            for task in test_procedure.schedule:
                for leaf_task in task:
                    try:
                        op_type = workload.OperationType.from_hyphenated_string(leaf_task.operation.type)
                    except KeyError:
                        op_type = None
                    if op_type == workload.OperationType.Search:
                        self.assertIsNotNone(leaf_task.operation.param_source)



# pylint: disable=too-many-public-methods
class WorkloadSpecificationReaderTests(TestCase):
    def test_description_is_optional(self):
        workload_specification = {
            # no description here
            "test_procedures": []
        }
        reader = loader.WorkloadSpecificationReader()

        resulting_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual("unittest", resulting_workload.name)
        self.assertEqual("", resulting_workload.description)

    def test_can_read_workload_info(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "types": ["test-type"]}],
            "data-streams": [],
            "corpora": [],
            "operations": [],
            "test_procedures": []
        }
        reader = loader.WorkloadSpecificationReader()
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual("unittest", resulting_workload.name)
        self.assertEqual("description for unit test", resulting_workload.description)

    def test_document_count_mandatory_if_file_present(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "types": ["docs"]}],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [{"source-file": "documents-main.json.bz2"}]
                }
            ],
            "test_procedures": []
        }
        reader = loader.WorkloadSpecificationReader()
        with self.assertRaises(loader.WorkloadSyntaxError) as ctx:
            reader("unittest", workload_specification, "/mappings")
        self.assertEqual("Workload 'unittest' is invalid. Mandatory element 'document-count' is missing.", ctx.exception.args[0])

    @mock.patch("osbenchmark.workload.loader.register_all_params_in_workload")
    def test_parse_with_mixed_warmup_iterations_and_measurement(self, mocked_params_checker):
        workload_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                    "body": "index.json",
                    "types": ["docs"]
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000
                        }
                    ]
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                    "bulk-size": 5000,
                }
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "warmup-iterations": 3,
                            "time-period": 60
                        }
                    ]
                }

            ]
        }

        reader = loader.WorkloadSpecificationReader(source=io.DictStringFileSourceFactory({
            "/mappings/index.json": ['{"mappings": {"docs": "empty-for-test"}}'],
        }))
        with self.assertRaises(loader.WorkloadSyntaxError) as ctx:
            reader("unittest", workload_specification, "/mappings")
        self.assertEqual("Workload 'unittest' is invalid. Operation 'index-append' in test_procedure 'default-test_procedure' "
                         "defines '3' warmup iterations and a time period of '60' seconds. Please do not mix time periods and iterations.",
                         ctx.exception.args[0])

    @mock.patch("osbenchmark.workload.loader.register_all_params_in_workload")
    def test_parse_missing_test_procedure_or_test_procedures(self, mocked_params_checker):
        workload_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                    "body": "index.json",
                    "types": ["docs"]
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000
                        }
                    ]
                }
            ],
            # no test_procedure or test_procedures element
        }
        reader = loader.WorkloadSpecificationReader(source=io.DictStringFileSourceFactory({
            "/mappings/index.json": ['{"mappings": {"docs": "empty-for-test"}}'],
        }))
        with self.assertRaises(loader.WorkloadSyntaxError) as ctx:
            reader("unittest", workload_specification, "/mappings")
        self.assertEqual("Workload 'unittest' is invalid. You must define 'test_procedure', 'test_procedures' or "
                         "'schedule' but none is specified.",
                         ctx.exception.args[0])

    @mock.patch("osbenchmark.workload.loader.register_all_params_in_workload")
    def test_parse_test_procedure_and_test_procedures_are_defined(self, mocked_params_checker):
        workload_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                    "body": "index.json",
                    "types": ["docs"]
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000
                        }
                    ]
                }
            ],
            # We define both. Note that test_procedures without any properties
            # would not pass JSON schema validation but we don't test this here.
            "test_procedure": {},
            "test_procedures": []
        }
        reader = loader.WorkloadSpecificationReader(source=io.DictStringFileSourceFactory({
            "/mappings/index.json": ['{"mappings": {"docs": "empty-for-test"}}'],
        }))
        with self.assertRaises(loader.WorkloadSyntaxError) as ctx:
            reader("unittest", workload_specification, "/mappings")
        self.assertEqual("Workload 'unittest' is invalid. Multiple out of 'test_procedure', 'test_procedures' or 'schedule' "
                         "are defined but only "
                         "one of them is allowed.", ctx.exception.args[0])

    @mock.patch("osbenchmark.workload.loader.register_all_params_in_workload")
    def test_parse_with_mixed_warmup_time_period_and_iterations(self, mocked_params_checker):
        workload_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                    "body": "index.json",
                    "types": ["docs"]
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000
                        }
                    ]
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index",
                    "bulk-size": 5000,
                }
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "warmup-time-period": 20,
                            "iterations": 1000
                        }
                    ]
                }

            ]
        }

        reader = loader.WorkloadSpecificationReader(source=io.DictStringFileSourceFactory({
            "/mappings/index.json": ['{"mappings": {"docs": "empty-for-test"}}'],
        }))
        with self.assertRaises(loader.WorkloadSyntaxError) as ctx:
            reader("unittest", workload_specification, "/mappings")
        self.assertEqual("Workload 'unittest' is invalid. Operation 'index-append' in test_procedure 'default-test_procedure' "
                         "defines a warmup time "
                         "period of '20' seconds and '1000' iterations. "
                         "Please do not mix time periods and iterations.",
                         ctx.exception.args[0])

    def test_parse_duplicate_implicit_task_names(self):
        workload_specification = {
            "description": "description for unit test",
            "operations": [
                {
                    "name": "search",
                    "operation-type": "search",
                    "index": "_all"
                }
            ],
            "test_procedure": {
                "name": "default-test_procedure",
                "schedule": [
                    {
                        "operation": "search",
                        "clients": 1
                    },
                    {
                        "operation": "search",
                        "clients": 2
                    }
                ]
            }
        }
        reader = loader.WorkloadSpecificationReader()
        with self.assertRaises(loader.WorkloadSyntaxError) as ctx:
            reader("unittest", workload_specification, "/mappings")
        self.assertEqual("Workload 'unittest' is invalid. TestProcedure 'default-test_procedure' contains multiple tasks"
                         " with the name 'search'. Please"
                         " use the task's name property to assign a unique name for each task.",
                         ctx.exception.args[0])

    def test_parse_duplicate_explicit_task_names(self):
        workload_specification = {
            "description": "description for unit test",
            "operations": [
                {
                    "name": "search",
                    "operation-type": "search",
                    "index": "_all"
                }
            ],
            "test_procedure": {
                "name": "default-test_procedure",
                "schedule": [
                    {
                        "name": "duplicate-task-name",
                        "operation": "search",
                        "clients": 1
                    },
                    {
                        "name": "duplicate-task-name",
                        "operation": "search",
                        "clients": 2
                    }
                ]
            }
        }
        reader = loader.WorkloadSpecificationReader()
        with self.assertRaises(loader.WorkloadSyntaxError) as ctx:
            reader("unittest", workload_specification, "/mappings")
        self.assertEqual("Workload 'unittest' is invalid. TestProcedure 'default-test_procedure' contains multiple tasks with the name "
                         "'duplicate-task-name'. Please use the task's name property to assign a unique name for each task.",
                         ctx.exception.args[0])

    @mock.patch("osbenchmark.workload.loader.register_all_params_in_workload")
    def test_load_invalid_index_body(self, mocked_params_checker):
        workload_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "index-historical",
                    "body": "body.json",
                    "types": ["_doc"]
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000
                        }
                    ]
                }
            ],
            "schedule": [
                {
                    "clients": 8,
                    "operation": {
                        "name": "index-append",
                        "operation-type": "index",
                        "bulk-size": 5000
                    }
                }
            ]
        }
        reader = loader.WorkloadSpecificationReader(
            workload_params={"number_of_shards": 3},
            source=io.DictStringFileSourceFactory({
                "/mappings/body.json": ["""
            {
                "settings": {
                    "number_of_shards": {{ number_of_shards }}
                },
                "mappings": {
                    "_doc": "no closing quotation mark!!,
                }
            }
            """]
            }))
        with self.assertRaises(loader.WorkloadSyntaxError) as ctx:
            reader("unittest", workload_specification, "/mappings")
        self.assertEqual("Could not load file template for 'definition for index index-historical in body.json'", ctx.exception.args[0])

    def test_parse_unique_task_names(self):
        workload_specification = {
            "description": "description for unit test",
            "operations": [
                {
                    "name": "search",
                    "operation-type": "search",
                    "index": "_all"
                }
            ],
            "test_procedure": {
                "name": "default-test_procedure",
                "schedule": [
                    {
                        "name": "search-one-client",
                        "operation": "search",
                        "clients": 1
                    },
                    {
                        "name": "search-two-clients",
                        "operation": "search",
                        "clients": 2
                    }
                ]
            }
        }
        reader = loader.WorkloadSpecificationReader(selected_test_procedure="default-test_procedure")
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual("unittest", resulting_workload.name)
        test_procedure = resulting_workload.test_procedures[0]
        self.assertTrue(test_procedure.selected)
        schedule = test_procedure.schedule
        self.assertEqual(2, len(schedule))
        self.assertEqual("search-one-client", schedule[0].name)
        self.assertEqual("search", schedule[0].operation.name)
        self.assertEqual("search-two-clients", schedule[1].name)
        self.assertEqual("search", schedule[1].operation.name)

    def test_parse_indices_valid_workload_specification(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "index-historical",
                    "body": "body.json",
                    "types": ["main", "secondary"]
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "meta": {
                        "test-corpus": True
                    },
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "target-index": "index-historical",
                            "target-type": "main",
                            "meta": {
                                "test-docs": True,
                                "role": "main"
                            }
                        },
                        {
                            "source-file": "documents-secondary.json.bz2",
                            "includes-action-and-meta-data": True,
                            "document-count": 20,
                            "compressed-bytes": 200,
                            "uncompressed-bytes": 20000,
                            "meta": {
                                "test-docs": True,
                                "role": "secondary"
                            }

                        }
                    ]
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index",
                    "bulk-size": 5000,
                    "meta": {
                        "append": True
                    }
                },
                {
                    "name": "search",
                    "operation-type": "search",
                    "index": "index-historical"
                }
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "description": "Default test_procedure",
                    "meta": {
                        "mixed": True,
                        "max-clients": 8
                    },
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "meta": {
                                "operation-index": 0
                            }
                        },
                        {
                            "clients": 1,
                            "operation": "search"
                        }
                    ]
                }
            ]
        }
        complete_workload_params = loader.CompleteWorkloadParams()
        reader = loader.WorkloadSpecificationReader(
            workload_params={"number_of_shards": 3},
            complete_workload_params=complete_workload_params,
            source=io.DictStringFileSourceFactory({
                "/mappings/body.json": ["""
            {
                "settings": {
                    "number_of_shards": {{ number_of_shards }}
                },
                "mappings": {
                    "main": "empty-for-test",
                    "secondary": "empty-for-test"
                }
            }
            """]
            }))
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        # j2 variables defined in the workload -- used for checking mismatching user workload params
        self.assertEqual(
            ["number_of_shards"],
            complete_workload_params.sorted_workload_defined_params
        )
        self.assertEqual("unittest", resulting_workload.name)
        self.assertEqual("description for unit test", resulting_workload.description)
        # indices
        self.assertEqual(1, len(resulting_workload.indices))
        self.assertEqual("index-historical", resulting_workload.indices[0].name)
        self.assertDictEqual({
            "settings": {
                "number_of_shards": 3
            },
            "mappings":
                {
                    "main": "empty-for-test",
                    "secondary": "empty-for-test"
                }
        }, resulting_workload.indices[0].body)
        self.assertEqual(2, len(resulting_workload.indices[0].types))
        self.assertEqual("main", resulting_workload.indices[0].types[0])
        self.assertEqual("secondary", resulting_workload.indices[0].types[1])
        # corpora
        self.assertEqual(1, len(resulting_workload.corpora))
        self.assertEqual("test", resulting_workload.corpora[0].name)
        self.assertDictEqual({"test-corpus": True}, resulting_workload.corpora[0].meta_data)
        self.assertEqual(2, len(resulting_workload.corpora[0].documents))

        docs_primary = resulting_workload.corpora[0].documents[0]
        self.assertEqual(workload.Documents.SOURCE_FORMAT_BULK, docs_primary.source_format)
        self.assertEqual("documents-main.json", docs_primary.document_file)
        self.assertEqual("documents-main.json.bz2", docs_primary.document_archive)
        self.assertEqual("https://localhost/data", docs_primary.base_url)
        self.assertFalse(docs_primary.includes_action_and_meta_data)
        self.assertEqual(10, docs_primary.number_of_documents)
        self.assertEqual(100, docs_primary.compressed_size_in_bytes)
        self.assertEqual(10000, docs_primary.uncompressed_size_in_bytes)
        self.assertEqual("index-historical", docs_primary.target_index)
        self.assertEqual("main", docs_primary.target_type)
        self.assertDictEqual({
            "test-docs": True,
            "role": "main"
        }, docs_primary.meta_data)

        docs_secondary = resulting_workload.corpora[0].documents[1]
        self.assertEqual(workload.Documents.SOURCE_FORMAT_BULK, docs_secondary.source_format)
        self.assertEqual("documents-secondary.json", docs_secondary.document_file)
        self.assertEqual("documents-secondary.json.bz2", docs_secondary.document_archive)
        self.assertEqual("https://localhost/data", docs_secondary.base_url)
        self.assertTrue(docs_secondary.includes_action_and_meta_data)
        self.assertEqual(20, docs_secondary.number_of_documents)
        self.assertEqual(200, docs_secondary.compressed_size_in_bytes)
        self.assertEqual(20000, docs_secondary.uncompressed_size_in_bytes)
        # This is defined by the action-and-meta-data line!
        self.assertIsNone(docs_secondary.target_index)
        self.assertIsNone(docs_secondary.target_type)
        self.assertDictEqual({
            "test-docs": True,
            "role": "secondary"
        }, docs_secondary.meta_data)

        # test_procedures
        self.assertEqual(1, len(resulting_workload.test_procedures))
        self.assertEqual("default-test_procedure", resulting_workload.test_procedures[0].name)
        self.assertEqual("Default test_procedure", resulting_workload.test_procedures[0].description)
        self.assertEqual({"mixed": True, "max-clients": 8}, resulting_workload.test_procedures[0].meta_data)
        self.assertEqual({"append": True}, resulting_workload.test_procedures[0].schedule[0].operation.meta_data)
        self.assertEqual({"operation-index": 0}, resulting_workload.test_procedures[0].schedule[0].meta_data)

    def test_parse_data_streams_valid_workload_specification(self):
        workload_specification = {
            "description": "description for unit test",
            "data-streams": [
                {
                    "name": "data-stream-historical"
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "target-data-stream": "data-stream-historical"
                        },
                        {
                            "source-file": "documents-secondary.json.bz2",
                            "includes-action-and-meta-data": True,
                            "document-count": 20,
                            "compressed-bytes": 200,
                            "uncompressed-bytes": 20000
                        },
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "target-data-stream": "data-stream-historical"
                        }
                    ]
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index",
                    "bulk-size": 5000,
                    "meta": {
                        "append": True
                    }
                },
                {
                    "name": "search",
                    "operation-type": "search",
                    "data-stream": "data-stream-historical"
                }
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "description": "Default test_procedure",
                    "meta": {
                        "mixed": True,
                        "max-clients": 8
                    },
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "meta": {
                                "operation-index": 0
                            }
                        },
                        {
                            "clients": 1,
                            "operation": "search"
                        }
                    ]
                }
            ]
        }
        complete_workload_params = loader.CompleteWorkloadParams()
        reader = loader.WorkloadSpecificationReader(
            complete_workload_params=complete_workload_params)
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        # j2 variables defined in the workload -- used for checking mismatching user workload params
        self.assertEqual("unittest", resulting_workload.name)
        self.assertEqual("description for unit test", resulting_workload.description)
        # data streams
        self.assertEqual(1, len(resulting_workload.data_streams))
        self.assertEqual("data-stream-historical", resulting_workload.data_streams[0].name)
        # corpora
        self.assertEqual(1, len(resulting_workload.corpora))
        self.assertEqual("test", resulting_workload.corpora[0].name)
        self.assertEqual(3, len(resulting_workload.corpora[0].documents))

        docs_primary = resulting_workload.corpora[0].documents[0]
        self.assertEqual(workload.Documents.SOURCE_FORMAT_BULK, docs_primary.source_format)
        self.assertEqual("documents-main.json", docs_primary.document_file)
        self.assertEqual("documents-main.json.bz2", docs_primary.document_archive)
        self.assertEqual("https://localhost/data", docs_primary.base_url)
        self.assertFalse(docs_primary.includes_action_and_meta_data)
        self.assertEqual(10, docs_primary.number_of_documents)
        self.assertEqual(100, docs_primary.compressed_size_in_bytes)
        self.assertEqual(10000, docs_primary.uncompressed_size_in_bytes)
        self.assertEqual("data-stream-historical", docs_primary.target_data_stream)
        self.assertIsNone(docs_primary.target_index)
        self.assertIsNone(docs_primary.target_type)

        docs_secondary = resulting_workload.corpora[0].documents[1]
        self.assertEqual(workload.Documents.SOURCE_FORMAT_BULK, docs_secondary.source_format)
        self.assertEqual("documents-secondary.json", docs_secondary.document_file)
        self.assertEqual("documents-secondary.json.bz2", docs_secondary.document_archive)
        self.assertEqual("https://localhost/data", docs_secondary.base_url)
        self.assertTrue(docs_secondary.includes_action_and_meta_data)
        self.assertEqual(20, docs_secondary.number_of_documents)
        self.assertEqual(200, docs_secondary.compressed_size_in_bytes)
        self.assertEqual(20000, docs_secondary.uncompressed_size_in_bytes)
        # This is defined by the action-and-meta-data line!
        self.assertIsNone(docs_secondary.target_data_stream)
        self.assertIsNone(docs_secondary.target_index)
        self.assertIsNone(docs_secondary.target_type)

        docs_tertiary = resulting_workload.corpora[0].documents[2]
        self.assertEqual(workload.Documents.SOURCE_FORMAT_BULK, docs_tertiary.source_format)
        self.assertEqual("documents-main.json", docs_tertiary.document_file)
        self.assertEqual("documents-main.json.bz2", docs_tertiary.document_archive)
        self.assertEqual("https://localhost/data", docs_tertiary.base_url)
        self.assertFalse(docs_tertiary.includes_action_and_meta_data)
        self.assertEqual(10, docs_tertiary.number_of_documents)
        self.assertEqual(100, docs_tertiary.compressed_size_in_bytes)
        self.assertIsNone(docs_tertiary.target_index)
        self.assertIsNone(docs_tertiary.target_type)
        self.assertEqual("data-stream-historical", docs_tertiary.target_data_stream)

        # test_procedures
        self.assertEqual(1, len(resulting_workload.test_procedures))
        self.assertEqual("default-test_procedure", resulting_workload.test_procedures[0].name)
        self.assertEqual("Default test_procedure", resulting_workload.test_procedures[0].description)
        self.assertEqual({"mixed": True, "max-clients": 8}, resulting_workload.test_procedures[0].meta_data)
        self.assertEqual({"append": True}, resulting_workload.test_procedures[0].schedule[0].operation.meta_data)
        self.assertEqual({"operation-index": 0}, resulting_workload.test_procedures[0].schedule[0].meta_data)

    @mock.patch("osbenchmark.workload.loader.register_all_params_in_workload")
    def test_parse_valid_without_types(self, mocked_param_checker):
        workload_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "index-historical",
                    "body": "body.json"
                    # no type information here
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                        },
                    ]
                }
            ],
            "schedule": [
                {
                    "clients": 8,
                    "operation": {
                        "name": "index-append",
                        "operation-type": "bulk",
                        "bulk-size": 5000
                    }
                }
            ]
        }
        reader = loader.WorkloadSpecificationReader(
            workload_params={"number_of_shards": 3},
            source=io.DictStringFileSourceFactory({
                "/mappings/body.json": ["""
            {
                "settings": {
                    "number_of_shards": {{ number_of_shards }}
                }
            }
            """]
            }))
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual("unittest", resulting_workload.name)
        self.assertEqual("description for unit test", resulting_workload.description)
        # indices
        self.assertEqual(1, len(resulting_workload.indices))
        self.assertEqual("index-historical", resulting_workload.indices[0].name)
        self.assertDictEqual({
            "settings": {
                "number_of_shards": 3
            }
        }, resulting_workload.indices[0].body)
        self.assertEqual(0, len(resulting_workload.indices[0].types))
        # corpora
        self.assertEqual(1, len(resulting_workload.corpora))
        self.assertEqual("test", resulting_workload.corpora[0].name)
        self.assertEqual(1, len(resulting_workload.corpora[0].documents))

        docs_primary = resulting_workload.corpora[0].documents[0]
        self.assertEqual(workload.Documents.SOURCE_FORMAT_BULK, docs_primary.source_format)
        self.assertEqual("documents-main.json", docs_primary.document_file)
        self.assertEqual("documents-main.json.bz2", docs_primary.document_archive)
        self.assertEqual("https://localhost/data", docs_primary.base_url)
        self.assertFalse(docs_primary.includes_action_and_meta_data)
        self.assertEqual(10, docs_primary.number_of_documents)
        self.assertEqual(100, docs_primary.compressed_size_in_bytes)
        self.assertEqual(10000, docs_primary.uncompressed_size_in_bytes)
        self.assertEqual("index-historical", docs_primary.target_index)
        self.assertIsNone(docs_primary.target_type)
        self.assertIsNone(docs_primary.target_data_stream)

        # test_procedures
        self.assertEqual(1, len(resulting_workload.test_procedures))

    @mock.patch("osbenchmark.workload.loader.register_all_params_in_workload")
    def test_parse_invalid_data_streams_with_indices(self, mocked_param_checker):
        workload_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "index-historical",
                    # no type information here
                }
            ],
            "data-streams": [
                {
                    "name": "historical-data-stream"
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                        },
                    ]
                }
            ],
            "schedule": [
                {
                    "clients": 8,
                    "operation": {
                        "name": "index-append",
                        "operation-type": "bulk",
                        "bulk-size": 5000
                    }
                }
            ]
        }
        complete_workload_params = loader.CompleteWorkloadParams()
        reader = loader.WorkloadSpecificationReader(
            complete_workload_params=complete_workload_params)
        with self.assertRaises(loader.WorkloadSyntaxError):
            reader("unittest", workload_specification, "/mapping")

    @mock.patch("osbenchmark.workload.loader.register_all_params_in_workload")
    def test_parse_invalid_data_streams_with_target_index(self, mocked_param_checker):
        workload_specification = {
            "description": "description for unit test",
            "data-streams": [
                {
                    "name": "historical-data-stream"
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "target-index": "historical-index",
                        },
                    ]
                }
            ],
            "schedule": [
                {
                    "clients": 8,
                    "operation": {
                        "name": "index-append",
                        "operation-type": "bulk",
                        "bulk-size": 5000
                    }
                }
            ]
        }
        complete_workload_params = loader.CompleteWorkloadParams()
        reader = loader.WorkloadSpecificationReader(
            complete_workload_params=complete_workload_params)
        with self.assertRaises(loader.WorkloadSyntaxError):
            reader("unittest", workload_specification, "/mapping")

    @mock.patch("osbenchmark.workload.loader.register_all_params_in_workload")
    def test_parse_invalid_data_streams_with_target_type(self, mocked_param_checker):
        workload_specification = {
            "description": "description for unit test",
            "data-streams": [
                {
                    "name": "historical-data-stream"
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "target-type": "_doc",
                        },
                    ]
                }
            ],
            "schedule": [
                {
                    "clients": 8,
                    "operation": {
                        "name": "index-append",
                        "operation-type": "bulk",
                        "bulk-size": 5000
                    }
                }
            ]
        }
        complete_workload_params = loader.CompleteWorkloadParams()
        reader = loader.WorkloadSpecificationReader(
            complete_workload_params=complete_workload_params)
        with self.assertRaises(loader.WorkloadSyntaxError):
            reader("unittest", workload_specification, "/mapping")

    @mock.patch("osbenchmark.workload.loader.register_all_params_in_workload")
    def test_parse_invalid_no_data_stream_target(self, mocked_param_checker):
        workload_specification = {
            "description": "description for unit test",
            "data-streams": [
                {
                    "name": "historical-data-stream"
                },
                {
                    "name": "historical-data-stream-2"
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000
                        }
                    ]
                }
            ],
            "schedule": [
                {
                    "clients": 8,
                    "operation": {
                        "name": "index-append",
                        "operation-type": "bulk",
                        "bulk-size": 5000
                    }
                }
            ]
        }
        complete_workload_params = loader.CompleteWorkloadParams()
        reader = loader.WorkloadSpecificationReader(
            complete_workload_params=complete_workload_params)
        with self.assertRaises(loader.WorkloadSyntaxError):
            reader("unittest", workload_specification, "/mapping")

    @mock.patch("osbenchmark.workload.loader.register_all_params_in_workload")
    def test_parse_valid_without_indices(self, mocked_param_checker):
        workload_specification = {
            "description": "description for unit test",
            "data-streams": [
                {
                    "name": "historical-data-stream"
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                        },
                    ]
                }
            ],
            "schedule": [
                {
                    "clients": 8,
                    "operation": {
                        "name": "index-append",
                        "operation-type": "bulk",
                        "bulk-size": 5000
                    }
                }
            ]
        }
        reader = loader.WorkloadSpecificationReader(
            workload_params={"number_of_shards": 3},
            source=io.DictStringFileSourceFactory({
                "/mappings/body.json": ["""
                {
                    "settings": {
                        "number_of_shards": {{ number_of_shards }}
                    }
                }
                """]
            }))
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual("unittest", resulting_workload.name)
        self.assertEqual("description for unit test", resulting_workload.description)
        # indices
        self.assertEqual(0, len(resulting_workload.indices))
        # data streams
        self.assertEqual(1, len(resulting_workload.data_streams))
        self.assertEqual("historical-data-stream", resulting_workload.data_streams[0].name)
        # corpora
        self.assertEqual(1, len(resulting_workload.corpora))
        self.assertEqual("test", resulting_workload.corpora[0].name)
        self.assertEqual(1, len(resulting_workload.corpora[0].documents))

        docs_primary = resulting_workload.corpora[0].documents[0]
        self.assertEqual(workload.Documents.SOURCE_FORMAT_BULK, docs_primary.source_format)
        self.assertEqual("documents-main.json", docs_primary.document_file)
        self.assertEqual("documents-main.json.bz2", docs_primary.document_archive)
        self.assertEqual("https://localhost/data", docs_primary.base_url)
        self.assertFalse(docs_primary.includes_action_and_meta_data)
        self.assertEqual(10, docs_primary.number_of_documents)
        self.assertEqual(100, docs_primary.compressed_size_in_bytes)
        self.assertEqual(10000, docs_primary.uncompressed_size_in_bytes)
        self.assertEqual("historical-data-stream", docs_primary.target_data_stream)
        self.assertIsNone(docs_primary.target_type)
        self.assertIsNone(docs_primary.target_index)

        # test_procedures
        self.assertEqual(1, len(resulting_workload.test_procedures))

    def test_parse_valid_workload_specification_with_index_template(self):
        workload_specification = {
            "description": "description for unit test",
            "templates": [
                {
                    "name": "my-index-template",
                    "index-pattern": "*",
                    "template": "default-template.json"
                }
            ],
            "operations": [],
            "test_procedures": []
        }
        complete_workload_params = loader.CompleteWorkloadParams()
        reader = loader.WorkloadSpecificationReader(
            workload_params={"index_pattern": "*"},
            complete_workload_params=complete_workload_params,
            source=io.DictStringFileSourceFactory({
                "/mappings/default-template.json": ["""
                {
                    "index_patterns": [ "{{index_pattern}}"],
                    "settings": {
                        "number_of_shards": {{ number_of_shards | default(1) }}
                    }
                }
                """],
        }))
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual(
            ["index_pattern", "number_of_shards"],
            complete_workload_params.sorted_workload_defined_params
        )
        self.assertEqual("unittest", resulting_workload.name)
        self.assertEqual("description for unit test", resulting_workload.description)
        self.assertEqual(0, len(resulting_workload.indices))
        self.assertEqual(1, len(resulting_workload.templates))
        self.assertEqual("my-index-template", resulting_workload.templates[0].name)
        self.assertEqual("*", resulting_workload.templates[0].pattern)
        self.assertDictEqual(
            {
                "index_patterns": ["*"],
                "settings": {
                    "number_of_shards": 1
                }
            }, resulting_workload.templates[0].content)
        self.assertEqual(0, len(resulting_workload.test_procedures))

    def test_parse_valid_workload_specification_with_composable_template(self):
        workload_specification = {
            "description": "description for unit test",
            "composable-templates": [
                {
                    "name": "my-index-template",
                    "index-pattern": "*",
                    "template": "default-template.json"
                }
            ],
            "component-templates": [
                {
                    "name": "my-component-template-1",
                    "template": "component-template-1.json"
                },
                {
                    "name": "my-component-template-2",
                    "template": "component-template-2.json"
                }
            ],
            "operations": [],
            "test_procedures": []
        }
        complete_workload_params = loader.CompleteWorkloadParams()
        reader = loader.WorkloadSpecificationReader(
            workload_params={"index_pattern": "logs-*", "number_of_replicas": 1},
            complete_workload_params=complete_workload_params,
            source=io.DictStringFileSourceFactory({
                "/mappings/default-template.json": ["""
                        {
                            "index_patterns": [ "{{index_pattern}}"],
                            "template": {
                                "settings": {
                                    "number_of_shards": {{ number_of_shards | default(1) }}
                                }
                            },
                            "composed_of": ["my-component-template-1", "my-component-template-2"]
                        }
                        """],
                "/mappings/component-template-1.json": ["""
                        {
                            "template": {
                                "settings": {
                                  "index.number_of_shards": 2
                                }
                            }
                        }
                        """],
                "/mappings/component-template-2.json": ["""
                        {
                            "template": {
                                "settings": {
                                  "index.number_of_replicas": {{ number_of_replicas }}
                                },
                                "mappings": {
                                  "properties": {
                                    "@timestamp": {
                                      "type": "date"
                                    }
                                  }
                                }
                              }
                        }
                        """]
            }))
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual(
            ["index_pattern", "number_of_replicas", "number_of_shards"],
            complete_workload_params.sorted_workload_defined_params
        )
        self.assertEqual("unittest", resulting_workload.name)
        self.assertEqual("description for unit test", resulting_workload.description)
        self.assertEqual(0, len(resulting_workload.indices))
        self.assertEqual(1, len(resulting_workload.composable_templates))
        self.assertEqual(2, len(resulting_workload.component_templates))
        self.assertEqual("my-index-template", resulting_workload.composable_templates[0].name)
        self.assertEqual("*", resulting_workload.composable_templates[0].pattern)
        self.assertEqual("my-component-template-1", resulting_workload.component_templates[0].name)
        self.assertEqual("my-component-template-2", resulting_workload.component_templates[1].name)
        self.assertDictEqual(
            {
                "index_patterns": ["logs-*"],
                "template": {
                    "settings": {
                        "number_of_shards": 1
                    }
                },
                "composed_of": ["my-component-template-1", "my-component-template-2"]
            }, resulting_workload.composable_templates[0].content)
        self.assertDictEqual(
            {
                "template": {
                    "settings": {
                        "index.number_of_shards": 2
                    }
                }
            }, resulting_workload.component_templates[0].content)
        self.assertDictEqual(
            {
                "template": {
                    "settings": {
                        "index.number_of_replicas": 1
                    },
                    "mappings": {
                        "properties": {
                            "@timestamp": {
                                "type": "date"
                            }
                        }
                    }
                }
            }, resulting_workload.component_templates[1].content)
        self.assertEqual(0, len(resulting_workload.test_procedures))

    def test_parse_invalid_workload_specification_with_composable_template(self):
        workload_specification = {
            "description": "description for unit test",
            "component-templates": [
                {
                    "name": "my-component-template-2"
                }
            ],
            "operations": [],
            "test_procedures": []
        }
        complete_workload_params = loader.CompleteWorkloadParams()
        reader = loader.WorkloadSpecificationReader(
            workload_params={"index_pattern": "logs-*", "number_of_replicas": 1},
            complete_workload_params=complete_workload_params)
        with self.assertRaises(loader.WorkloadSyntaxError) as ctx:
            reader("unittest", workload_specification, "/mappings")
        self.assertEqual("Workload 'unittest' is invalid. Mandatory element 'template' is missing.",
                         ctx.exception.args[0])

    def test_unique_test_procedure_names(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk"
                }
            ],
            "test_procedures": [
                {
                    "name": "test-test_procedure",
                    "description": "Some test_procedure",
                    "default": True,
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                },
                {
                    "name": "test-test_procedure",
                    "description": "Another test_procedure with the same name",
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                }

            ]
        }
        reader = loader.WorkloadSpecificationReader()
        with self.assertRaises(loader.WorkloadSyntaxError) as ctx:
            reader("unittest", workload_specification, "/mappings")
        self.assertEqual("Workload 'unittest' is invalid. Duplicate test_procedure with name 'test-test_procedure'.", ctx.exception.args[0])

    def test_not_more_than_one_default_test_procedure_possible(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk"
                }
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "description": "Default test_procedure",
                    "default": True,
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                },
                {
                    "name": "another-test_procedure",
                    "description": "See if we can sneek it in as another default",
                    "default": True,
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                }

            ]
        }
        reader = loader.WorkloadSpecificationReader()
        with self.assertRaises(loader.WorkloadSyntaxError) as ctx:
            reader("unittest", workload_specification, "/mappings")
        self.assertEqual("Workload 'unittest' is invalid. Both 'default-test_procedure' and 'another-test_procedure' "
                         "are defined as default test_procedures. "
                         "Please define only one of them as default.", ctx.exception.args[0])

    def test_at_least_one_default_test_procedure(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk"
                }
            ],
            "test_procedures": [
                {
                    "name": "test_procedure",
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                },
                {
                    "name": "another-test_procedure",
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                }

            ]
        }
        reader = loader.WorkloadSpecificationReader()
        with self.assertRaises(loader.WorkloadSyntaxError) as ctx:
            reader("unittest", workload_specification, "/mappings")
        self.assertEqual("Workload 'unittest' is invalid. No default test_procedure specified. "
                         "Please edit the workload and add \"default\": true "
                         "to one of the test_procedures test_procedure, another-test_procedure.", ctx.exception.args[0])

    def test_exactly_one_default_test_procedure(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk"
                }
            ],
            "test_procedures": [
                {
                    "name": "test_procedure",
                    "default": True,
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                },
                {
                    "name": "another-test_procedure",
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                }

            ]
        }
        reader = loader.WorkloadSpecificationReader(selected_test_procedure="another-test_procedure")
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual(2, len(resulting_workload.test_procedures))
        self.assertEqual("test_procedure", resulting_workload.test_procedures[0].name)
        self.assertTrue(resulting_workload.test_procedures[0].default)
        self.assertFalse(resulting_workload.test_procedures[1].default)
        self.assertTrue(resulting_workload.test_procedures[1].selected)

    def test_selects_sole_test_procedure_implicitly_as_default(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk"
                }
            ],
            "test_procedure": {
                "name": "test_procedure",
                "schedule": [
                    {
                        "operation": "index-append"
                    }
                ]
            }
        }
        reader = loader.WorkloadSpecificationReader()
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual(1, len(resulting_workload.test_procedures))
        self.assertEqual("test_procedure", resulting_workload.test_procedures[0].name)
        self.assertTrue(resulting_workload.test_procedures[0].default)
        self.assertTrue(resulting_workload.test_procedures[0].selected)

    def test_auto_generates_test_procedure_from_schedule(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk"
                }
            ],
            "schedule": [
                {
                    "operation": "index-append"
                }
            ]
        }
        reader = loader.WorkloadSpecificationReader()
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual(1, len(resulting_workload.test_procedures))
        self.assertTrue(resulting_workload.test_procedures[0].auto_generated)
        self.assertTrue(resulting_workload.test_procedures[0].default)
        self.assertTrue(resulting_workload.test_procedures[0].selected)

    def test_inline_operations(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "test_procedure": {
                "name": "test_procedure",
                "schedule": [
                    # an operation with parameters still needs to define a type
                    {
                        "operation": {
                            "operation-type": "bulk",
                            "bulk-size": 5000
                        }
                    },
                    # a parameterless operation can just use the operation type as implicit reference to the operation
                    {
                        "operation": "force-merge"
                    }
                ]
            }
        }
        reader = loader.WorkloadSpecificationReader()
        resulting_workload = reader("unittest", workload_specification, "/mappings")

        test_procedure = resulting_workload.test_procedures[0]
        self.assertEqual(2, len(test_procedure.schedule))
        self.assertEqual(workload.OperationType.Bulk.to_hyphenated_string(), test_procedure.schedule[0].operation.type)
        self.assertEqual(workload.OperationType.ForceMerge.to_hyphenated_string(), test_procedure.schedule[1].operation.type)

    def test_supports_target_throughput(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk"
                }
            ],
            "test_procedure": {
                "name": "default-test_procedure",
                "schedule": [
                    {
                        "operation": "index-append",
                        "target-throughput": 10,
                    }
                ]
            }
        }
        reader = loader.WorkloadSpecificationReader()
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual(10, resulting_workload.test_procedures[0].schedule[0].params["target-throughput"])

    def test_supports_target_interval(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk"
                }
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "schedule": [
                        {
                            "operation": "index-append",
                            "target-interval": 5,
                        }
                    ]
                }
            ]
        }
        reader = loader.WorkloadSpecificationReader()
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual(5, resulting_workload.test_procedures[0].schedule[0].params["target-interval"])

    def test_parallel_tasks_with_default_values(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "bulk"
                },
                {
                    "name": "index-2",
                    "operation-type": "bulk"
                },
                {
                    "name": "index-3",
                    "operation-type": "bulk"
                },
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "schedule": [
                        {
                            "parallel": {
                                "warmup-time-period": 2400,
                                "time-period": 36000,
                                "tasks": [
                                    {
                                        "operation": "index-1",
                                        "warmup-time-period": 300,
                                        "clients": 2
                                    },
                                    {
                                        "operation": "index-2",
                                        "time-period": 3600,
                                        "clients": 4
                                    },
                                    {
                                        "operation": "index-3",
                                        "target-throughput": 10,
                                        "clients": 16
                                    },
                                ]
                            }
                        }
                    ]
                }
            ]
        }
        reader = loader.WorkloadSpecificationReader()
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        parallel_element = resulting_workload.test_procedures[0].schedule[0]
        parallel_tasks = parallel_element.tasks

        self.assertEqual(22, parallel_element.clients)
        self.assertEqual(3, len(parallel_tasks))

        self.assertEqual("index-1", parallel_tasks[0].operation.name)
        self.assertEqual(300, parallel_tasks[0].warmup_time_period)
        self.assertEqual(36000, parallel_tasks[0].time_period)
        self.assertEqual(2, parallel_tasks[0].clients)
        self.assertFalse("target-throughput" in parallel_tasks[0].params)

        self.assertEqual("index-2", parallel_tasks[1].operation.name)
        self.assertEqual(2400, parallel_tasks[1].warmup_time_period)
        self.assertEqual(3600, parallel_tasks[1].time_period)
        self.assertEqual(4, parallel_tasks[1].clients)
        self.assertFalse("target-throughput" in parallel_tasks[1].params)

        self.assertEqual("index-3", parallel_tasks[2].operation.name)
        self.assertEqual(2400, parallel_tasks[2].warmup_time_period)
        self.assertEqual(36000, parallel_tasks[2].time_period)
        self.assertEqual(16, parallel_tasks[2].clients)
        self.assertEqual(10, parallel_tasks[2].params["target-throughput"])

    def test_parallel_tasks_with_default_clients_does_not_propagate(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "bulk"
                }
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "schedule": [
                        {
                            "parallel": {
                                "warmup-time-period": 2400,
                                "time-period": 36000,
                                "clients": 2,
                                "tasks": [
                                    {
                                        "name": "index-1-1",
                                        "operation": "index-1"
                                    },
                                    {
                                        "name": "index-1-2",
                                        "operation": "index-1"
                                    },
                                    {
                                        "name": "index-1-3",
                                        "operation": "index-1"
                                    },
                                    {
                                        "name": "index-1-4",
                                        "operation": "index-1"
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
        reader = loader.WorkloadSpecificationReader()
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        parallel_element = resulting_workload.test_procedures[0].schedule[0]
        parallel_tasks = parallel_element.tasks

        # we will only have two clients *in total*
        self.assertEqual(2, parallel_element.clients)
        self.assertEqual(4, len(parallel_tasks))
        for task in parallel_tasks:
            self.assertEqual(1, task.clients)

    def test_parallel_tasks_with_completed_by_set(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "bulk"
                },
                {
                    "name": "index-2",
                    "operation-type": "bulk"
                }
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "schedule": [
                        {
                            "parallel": {
                                "warmup-time-period": 2400,
                                "time-period": 36000,
                                "completed-by": "index-2",
                                "tasks": [
                                    {
                                        "operation": "index-1"
                                    },
                                    {
                                        "operation": "index-2"
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
        reader = loader.WorkloadSpecificationReader()
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        parallel_element = resulting_workload.test_procedures[0].schedule[0]
        parallel_tasks = parallel_element.tasks

        # we will only have two clients *in total*
        self.assertEqual(2, parallel_element.clients)
        self.assertEqual(2, len(parallel_tasks))

        self.assertEqual("index-1", parallel_tasks[0].operation.name)
        self.assertFalse(parallel_tasks[0].completes_parent)

        self.assertEqual("index-2", parallel_tasks[1].operation.name)
        self.assertTrue(parallel_tasks[1].completes_parent)

    def test_parallel_tasks_with_named_task_completed_by_set(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "bulk"
                },
                {
                    "name": "index-2",
                    "operation-type": "bulk"
                }
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "schedule": [
                        {
                            "parallel": {
                                "warmup-time-period": 2400,
                                "time-period": 36000,
                                "completed-by": "name-index-2",
                                "tasks": [
                                    {
                                        "name": "name-index-1",
                                        "operation": "index-1"
                                    },
                                    {
                                        "name": "name-index-2",
                                        "operation": "index-2"
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
        reader = loader.WorkloadSpecificationReader()
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        parallel_element = resulting_workload.test_procedures[0].schedule[0]
        parallel_tasks = parallel_element.tasks

        # we will only have two clients *in total*
        self.assertEqual(2, parallel_element.clients)
        self.assertEqual(2, len(parallel_tasks))

        self.assertEqual("index-1", parallel_tasks[0].operation.name)
        self.assertFalse(parallel_tasks[0].completes_parent)

        self.assertEqual("index-2", parallel_tasks[1].operation.name)
        self.assertTrue(parallel_tasks[1].completes_parent)

    def test_parallel_tasks_with_completed_by_set_no_task_matches(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "bulk"
                },
                {
                    "name": "index-2",
                    "operation-type": "bulk"
                }
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "schedule": [
                        {
                            "parallel": {
                                "completed-by": "non-existing-task",
                                "tasks": [
                                    {
                                        "operation": "index-1"
                                    },
                                    {
                                        "operation": "index-2"
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
        reader = loader.WorkloadSpecificationReader()

        with self.assertRaises(loader.WorkloadSyntaxError) as ctx:
            reader("unittest", workload_specification, "/mappings")
        self.assertEqual("Workload 'unittest' is invalid. 'parallel' element for "
                         "test_procedure 'default-test_procedure' is marked with 'completed-by' "
                         "with task name 'non-existing-task' but no task with this name exists.", ctx.exception.args[0])

    def test_parallel_tasks_with_completed_by_set_multiple_tasks_match(self):
        workload_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "bulk"
                }
            ],
            "test_procedures": [
                {
                    "name": "default-test_procedure",
                    "schedule": [
                        {
                            "parallel": {
                                "completed-by": "index-1",
                                "tasks": [
                                    {
                                        "operation": "index-1"
                                    },
                                    {
                                        "operation": "index-1"
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
        reader = loader.WorkloadSpecificationReader()

        with self.assertRaises(loader.WorkloadSyntaxError) as ctx:
            reader("unittest", workload_specification, "/mappings")
        self.assertEqual("Workload 'unittest' is invalid. 'parallel' element for test_procedure "
                         "'default-test_procedure' contains multiple tasks with "
                         "the name 'index-1' which are marked with 'completed-by' but only task is allowed to match.",
                         ctx.exception.args[0])

    def test_propagate_parameters_to_test_procedure_level(self):
        workload_specification = {
            "description": "description for unit test",
            "parameters": {
                "level": "workload",
                "value": 7
            },
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk"
                }
            ],
            "test_procedures": [
                {
                    "name": "test_procedure",
                    "default": True,
                    "parameters": {
                        "level": "test_procedure",
                        "another-value": 17
                    },
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                },
                {
                    "name": "another-test_procedure",
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                }

            ]
        }
        reader = loader.WorkloadSpecificationReader(selected_test_procedure="another-test_procedure")
        resulting_workload = reader("unittest", workload_specification, "/mappings")
        self.assertEqual(2, len(resulting_workload.test_procedures))
        self.assertEqual("test_procedure", resulting_workload.test_procedures[0].name)
        self.assertTrue(resulting_workload.test_procedures[0].default)
        self.assertDictEqual({
            "level": "test_procedure",
            "value": 7,
            "another-value": 17
        }, resulting_workload.test_procedures[0].parameters)

        self.assertFalse(resulting_workload.test_procedures[1].default)
        self.assertTrue(resulting_workload.test_procedures[1].selected)
        self.assertDictEqual({
            "level": "workload",
            "value": 7
        }, resulting_workload.test_procedures[1].parameters)


class MyMockWorkloadProcessor(loader.WorkloadProcessor):
    pass


class WorkloadProcessorRegistryTests(TestCase):
    def test_default_workload_processors(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "offline.mode", False)
        tpr = loader.WorkloadProcessorRegistry(cfg)
        expected_defaults = [
            loader.TaskFilterWorkloadProcessor,
            loader.TestModeWorkloadProcessor,
            loader.QueryRandomizerWorkloadProcessor,
            loader.DefaultWorkloadPreparator
        ]
        actual_defaults = [proc.__class__ for proc in tpr.processors]
        self.assertCountEqual(expected_defaults, actual_defaults)

    def test_override_default_preparator(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "offline.mode", False)
        tpr = loader.WorkloadProcessorRegistry(cfg)
        # call this once beforehand to make sure we don't "harden" the default in case calls are made out of order
        tpr.processors # pylint: disable=pointless-statement
        tpr.register_workload_processor(MyMockWorkloadProcessor())
        expected_processors = [
            loader.TaskFilterWorkloadProcessor,
            loader.TestModeWorkloadProcessor,
            loader.QueryRandomizerWorkloadProcessor,
            MyMockWorkloadProcessor
        ]
        actual_processors = [proc.__class__ for proc in tpr.processors]
        self.assertCountEqual(expected_processors, actual_processors)

    def test_allow_to_specify_default_preparator(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "offline.mode", False)
        tpr = loader.WorkloadProcessorRegistry(cfg)
        tpr.register_workload_processor(MyMockWorkloadProcessor())
        # should be idempotent now that we have a custom config
        tpr.processors # pylint: disable=pointless-statement
        tpr.register_workload_processor(loader.DefaultWorkloadPreparator(cfg))
        expected_processors = [
            loader.TaskFilterWorkloadProcessor,
            loader.TestModeWorkloadProcessor,
            loader.QueryRandomizerWorkloadProcessor,
            MyMockWorkloadProcessor,
            loader.DefaultWorkloadPreparator
        ]
        actual_processors = [proc.__class__ for proc in tpr.processors]
        self.assertCountEqual(expected_processors, actual_processors)
