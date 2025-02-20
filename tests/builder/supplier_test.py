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
# pylint: disable=protected-access

import collections
import datetime
import unittest.mock as mock
from unittest import TestCase

from osbenchmark import exceptions, config
from osbenchmark.builder import supplier, provision_config


class RevisionExtractorTests(TestCase):
    def test_single_revision(self):
        self.assertDictEqual({"opensearch": "67c2f42", "all": "67c2f42"}, supplier._extract_revisions("67c2f42"))
        self.assertDictEqual({"opensearch": "current", "all": "current"}, supplier._extract_revisions("current"))
        self.assertDictEqual({"opensearch": "@2015-01-01-01:00:00", "all": "@2015-01-01-01:00:00"},
                             supplier._extract_revisions("@2015-01-01-01:00:00"))

    def test_multiple_revisions(self):
        self.assertDictEqual({"opensearch": "67c2f42", "some-plugin": "current"},
                             supplier._extract_revisions("opensearch:67c2f42,some-plugin:current"))

    def test_invalid_revisions(self):
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            supplier._extract_revisions("opensearch 67c2f42,some-plugin:current")
        self.assertEqual("Revision [opensearch 67c2f42] does not match expected format [name:revision].", ctx.exception.args[0])


class SourceRepositoryTests(TestCase):
    @mock.patch("osbenchmark.utils.git.head_revision", autospec=True)
    @mock.patch("osbenchmark.utils.git.pull", autospec=True)
    @mock.patch("osbenchmark.utils.git.clone", autospec=True)
    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    def test_intial_checkout_latest(self, mock_is_working_copy, mock_clone, mock_pull, mock_head_revision):
        # before cloning, it is not a working copy, afterwards it is
        mock_is_working_copy.side_effect = [False, True]
        mock_head_revision.return_value = "HEAD"

        s = supplier.SourceRepository(name="OpenSearch", remote_url="some-github-url", src_dir="/src")
        s.fetch("latest")

        mock_is_working_copy.assert_called_with("/src")
        mock_clone.assert_called_with("/src", "some-github-url")
        mock_pull.assert_called_with("/src")
        mock_head_revision.assert_called_with("/src")

    @mock.patch("osbenchmark.utils.git.head_revision", autospec=True)
    @mock.patch("osbenchmark.utils.git.pull")
    @mock.patch("osbenchmark.utils.git.clone")
    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    def test_checkout_current(self, mock_is_working_copy, mock_clone, mock_pull, mock_head_revision):
        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        s = supplier.SourceRepository(name="OpenSearch", remote_url="some-github-url", src_dir="/src")
        s.fetch("current")

        mock_is_working_copy.assert_called_with("/src")
        self.assertEqual(0, mock_clone.call_count)
        self.assertEqual(0, mock_pull.call_count)
        mock_head_revision.assert_called_with("/src")\


    @mock.patch("osbenchmark.utils.git.head_revision", autospec=True)
    @mock.patch("osbenchmark.utils.git.checkout")
    @mock.patch("osbenchmark.utils.git.pull")
    @mock.patch("osbenchmark.utils.git.clone")
    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    def test_checkout_revision_for_local_only_repo(self, mock_is_working_copy, mock_clone, mock_pull, mock_checkout, mock_head_revision):
        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        # local only, we dont specify a remote
        s = supplier.SourceRepository(name="OpenSearch", remote_url=None, src_dir="/src")
        s.fetch("67c2f42")

        mock_is_working_copy.assert_called_with("/src")
        self.assertEqual(0, mock_clone.call_count)
        self.assertEqual(0, mock_pull.call_count)
        mock_checkout.assert_called_with("/src", "67c2f42")
        mock_head_revision.assert_called_with("/src")

    @mock.patch("osbenchmark.utils.git.head_revision", autospec=True)
    @mock.patch("osbenchmark.utils.git.pull_ts", autospec=True)
    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    def test_checkout_ts(self, mock_is_working_copy, mock_pull_ts, mock_head_revision):
        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        s = supplier.SourceRepository(name="OpenSearch", remote_url="some-github-url", src_dir="/src")
        s.fetch("@2015-01-01-01:00:00")

        mock_is_working_copy.assert_called_with("/src")
        mock_pull_ts.assert_called_with("/src", "2015-01-01-01:00:00")
        mock_head_revision.assert_called_with("/src")

    @mock.patch("osbenchmark.utils.git.head_revision", autospec=True)
    @mock.patch("osbenchmark.utils.git.pull_revision", autospec=True)
    @mock.patch("osbenchmark.utils.git.is_working_copy", autospec=True)
    def test_checkout_revision(self, mock_is_working_copy, mock_pull_revision, mock_head_revision):
        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        s = supplier.SourceRepository(name="OpenSearch", remote_url="some-github-url", src_dir="/src")
        s.fetch("67c2f42")

        mock_is_working_copy.assert_called_with("/src")
        mock_pull_revision.assert_called_with("/src", "67c2f42")
        mock_head_revision.assert_called_with("/src")

    def test_is_commit_hash(self):
        self.assertTrue(supplier.SourceRepository.is_commit_hash("67c2f42"))

    def test_is_not_commit_hash(self):
        self.assertFalse(supplier.SourceRepository.is_commit_hash("latest"))
        self.assertFalse(supplier.SourceRepository.is_commit_hash("current"))
        self.assertFalse(supplier.SourceRepository.is_commit_hash("@2015-01-01-01:00:00"))


class BuilderTests(TestCase):
    @mock.patch("osbenchmark.utils.process.run_subprocess")
    @mock.patch("osbenchmark.utils.jvm.resolve_path")
    def test_build_on_jdk_8(self, jvm_resolve_path, mock_run_subprocess):
        jvm_resolve_path.return_value = (8, "/opt/jdk8")
        mock_run_subprocess.return_value = False

        b = supplier.Builder(src_dir="/src", build_jdk=8, log_dir="logs")
        b.build(["./gradlew clean", "./gradlew assemble"])

        calls = [
            # Actual call
            mock.call("export JAVA_HOME=/opt/jdk8; cd /src; ./gradlew clean < /dev/null > logs/build.log 2>&1"),
            # Return value check
            mock.call("export JAVA_HOME=/opt/jdk8; cd /src; ./gradlew assemble < /dev/null > logs/build.log 2>&1"),
        ]

        mock_run_subprocess.assert_has_calls(calls)

    @mock.patch("osbenchmark.utils.process.run_subprocess")
    @mock.patch("osbenchmark.utils.jvm.resolve_path")
    def test_build_on_jdk_10(self, jvm_resolve_path, mock_run_subprocess):
        jvm_resolve_path.return_value = (10, "/opt/jdk10")
        mock_run_subprocess.return_value = False

        b = supplier.Builder(src_dir="/src", build_jdk=8, log_dir="logs")
        b.build(["./gradlew clean", "./gradlew assemble"])

        calls = [
            # Actual call
            mock.call("export JAVA_HOME=/opt/jdk10; cd /src; ./gradlew clean < /dev/null > logs/build.log 2>&1"),
            # Return value check
            mock.call("export JAVA_HOME=/opt/jdk10; cd /src; ./gradlew assemble < /dev/null > logs/build.log 2>&1"),
        ]

        mock_run_subprocess.assert_has_calls(calls)


class TemplateRendererTests(TestCase):
    def test_uses_provided_values(self):
        renderer = supplier.TemplateRenderer(version="1.2.3", os_name="Windows", arch="arm7")
        self.assertEqual("This is version 1.2.3 on Windows with a arm7 CPU.",
                         renderer.render("This is version {{VERSION}} on {{OSNAME}} with a {{ARCH}} CPU."))

    @mock.patch("osbenchmark.utils.sysstats.os_name", return_value="Linux")
    @mock.patch("osbenchmark.utils.sysstats.cpu_arch", return_value="X86_64")
    def test_uses_derived_values(self, os_name, cpu_arch):
        renderer = supplier.TemplateRenderer(version="1.2.3")
        self.assertEqual("This is version 1.2.3 on linux with a x86_64 CPU.",
                         renderer.render("This is version {{VERSION}} on {{OSNAME}} with a {{ARCH}} CPU."))

    @mock.patch("osbenchmark.utils.sysstats.os_name", return_value="Linux")
    @mock.patch("osbenchmark.utils.sysstats.cpu_arch", return_value="X86_64")
    def test_supported_os_enum_match(self, os_name, cpu_arch):
        renderer = supplier.TemplateRenderer(version="1.2.3")
        self.assertEqual("This is version 1.2.3 on linux with a x86_64 CPU.",
                         renderer.render("This is version {{VERSION}} on {{OSNAME}} with a {{ARCH}} CPU."))

    @mock.patch("osbenchmark.utils.sysstats.os_name", return_value="Darwin")
    @mock.patch("osbenchmark.utils.sysstats.cpu_arch", return_value="X86_64")
    def test_supported_os_enum_returns_default(self, os_name, cpu_arch):
        renderer = supplier.TemplateRenderer(version="1.2.3")
        self.assertEqual("This is version 1.2.3 on linux with a x86_64 CPU.",
                         renderer.render("This is version {{VERSION}} on {{OSNAME}} with a {{ARCH}} CPU."))


class CachedOpenSearchSourceSupplierTests(TestCase):
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("shutil.copy")
    @mock.patch("osbenchmark.builder.supplier.OpenSearchSourceSupplier")
    def test_does_not_cache_when_no_revision(self, opensearch, copy, ensure_dir):
        def add_os_artifact(binaries):
            binaries["opensearch"] = "/path/to/artifact.tar.gz"

        opensearch.fetch.return_value = None
        opensearch.add.side_effect = add_os_artifact

        # no version / revision provided
        renderer = supplier.TemplateRenderer(version=None, os_name="linux", arch="x64")

        dist_cfg = {
            "runtime.jdk.bundled": "true",
            "jdk.bundled.release_url":
                "https://artifacts.opensearch.org/releases/bundle/opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz"
        }
        file_resolver = supplier.OpenSearchFileNameResolver(
            distribution_config=dist_cfg,
            template_renderer=renderer
        )
        cached_supplier = supplier.CachedSourceSupplier(distributions_root="/tmp",
                                                        source_supplier=opensearch,
                                                        file_resolver=file_resolver)

        cached_supplier.fetch()
        cached_supplier.prepare()

        binaries = {}

        cached_supplier.add(binaries)

        self.assertEqual(0, copy.call_count)
        self.assertFalse(cached_supplier.cached)
        self.assertIn("opensearch", binaries)
        self.assertEqual("/path/to/artifact.tar.gz", binaries["opensearch"])

    @mock.patch("os.path.exists")
    @mock.patch("osbenchmark.builder.supplier.OpenSearchSourceSupplier")
    def test_uses_already_cached_artifact(self, opensearch, path_exists):
        # assume that the artifact is already cached
        path_exists.return_value = True
        renderer = supplier.TemplateRenderer(version="abc123", os_name="linux", arch="x64")

        dist_cfg = {
            "runtime.jdk.bundled": "true",
            "jdk.bundled.release_url":
                "https://artifacts.opensearch.org/releases/bundle/opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz"
        }
        file_resolver = supplier.OpenSearchFileNameResolver(
            distribution_config=dist_cfg,
            template_renderer=renderer
        )
        cached_supplier = supplier.CachedSourceSupplier(distributions_root="/tmp",
                                                        source_supplier=opensearch,
                                                        file_resolver=file_resolver)

        cached_supplier.fetch()
        cached_supplier.prepare()

        binaries = {}

        cached_supplier.add(binaries)

        self.assertEqual(0, opensearch.fetch.call_count)
        self.assertEqual(0, opensearch.prepare.call_count)
        self.assertEqual(0, opensearch.add.call_count)
        self.assertTrue(cached_supplier.cached)
        self.assertIn("opensearch", binaries)
        self.assertEqual("/tmp/opensearch-abc123-linux-x64.tar.gz", binaries["opensearch"])

    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("os.path.exists")
    @mock.patch("shutil.copy")
    @mock.patch("osbenchmark.builder.supplier.OpenSearchSourceSupplier")
    def test_caches_artifact(self, opensearch, copy, path_exists, ensure_dir):
        def add_os_artifact(binaries):
            binaries["opensearch"] = "/path/to/artifact.tar.gz"

        path_exists.return_value = False

        opensearch.fetch.return_value = "abc123"
        opensearch.add.side_effect = add_os_artifact

        renderer = supplier.TemplateRenderer(version="abc123", os_name="linux", arch="x64")

        dist_cfg = {
            "runtime.jdk.bundled": "true",
            "jdk.bundled.release_url":
                "https://artifacts.opensearch.org/releases/bundle/opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz"
        }

        cached_supplier = supplier.CachedSourceSupplier(distributions_root="/tmp",
                                                        source_supplier=opensearch,
                                                        file_resolver=supplier.OpenSearchFileNameResolver(
                                                            distribution_config=dist_cfg,
                                                            template_renderer=renderer
                                                        ))
        cached_supplier.fetch()
        cached_supplier.prepare()

        binaries = {}

        cached_supplier.add(binaries)
        # path is cached now
        path_exists.return_value = True

        self.assertEqual(1, copy.call_count, "artifact has been copied")
        self.assertEqual(1, opensearch.add.call_count, "artifact has been added by internal supplier")
        self.assertTrue(cached_supplier.cached)
        self.assertIn("opensearch", binaries)

        # simulate a second attempt
        cached_supplier.fetch()
        cached_supplier.prepare()

        binaries = {}
        cached_supplier.add(binaries)

        self.assertEqual(1, copy.call_count, "artifact has not been copied twice")
        # the internal supplier did not get called again as we reuse the cached artifact
        self.assertEqual(1, opensearch.add.call_count, "internal supplier is not called again")
        self.assertTrue(cached_supplier.cached)

    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("os.path.exists")
    @mock.patch("shutil.copy")
    @mock.patch("osbenchmark.builder.supplier.OpenSearchSourceSupplier")
    def test_does_not_cache_on_copy_error(self, opensearch, copy, path_exists, ensure_dir):
        def add_os_artifact(binaries):
            binaries["opensearch"] = "/path/to/artifact.tar.gz"

        path_exists.return_value = False

        opensearch.fetch.return_value = "abc123"
        opensearch.add.side_effect = add_os_artifact
        copy.side_effect = OSError("no space left on device")

        renderer = supplier.TemplateRenderer(version="abc123", os_name="linux", arch="x64")

        dist_cfg = {
            "runtime.jdk.bundled": "true",
            "jdk.bundled.release_url":
                "https://artifacts.opensearch.org/releases/bundle/opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz"
        }

        cached_supplier = supplier.CachedSourceSupplier(distributions_root="/tmp",
                                                        source_supplier=opensearch,
                                                        file_resolver=supplier.OpenSearchFileNameResolver(
                                                            distribution_config=dist_cfg,
                                                            template_renderer=renderer
                                                        ))
        cached_supplier.fetch()
        cached_supplier.prepare()

        binaries = {}

        cached_supplier.add(binaries)

        self.assertEqual(1, copy.call_count, "artifact has been copied")
        self.assertEqual(1, opensearch.add.call_count, "artifact has been added by internal supplier")
        self.assertFalse(cached_supplier.cached)
        self.assertIn("opensearch", binaries)
        # still the uncached artifact
        self.assertEqual("/path/to/artifact.tar.gz", binaries["opensearch"])


class OpenSearchFileNameResolverTests(TestCase):
    def setUp(self):
        super().setUp()
        renderer = supplier.TemplateRenderer(version="8.0.0-SNAPSHOT", os_name="linux", arch="x86_64")

        dist_cfg = {
            "runtime.jdk.bundled": "true",
            "jdk.bundled.release_url":
                "https://artifacts.opensearch.org/releases/bundle/opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz"
        }

        self.resolver = supplier.OpenSearchFileNameResolver(
            distribution_config=dist_cfg,
            template_renderer=renderer
        )

    def test_resolve(self):
        self.resolver.revision = "abc123"
        self.assertEqual("opensearch-abc123-linux-x86_64.tar.gz", self.resolver.file_name)

    def test_artifact_key(self):
        self.assertEqual("opensearch", self.resolver.artifact_key)

    def test_to_artifact_path(self):
        file_system_path = "/tmp/test"
        self.assertEqual(file_system_path, self.resolver.to_artifact_path(file_system_path))

    def test_to_file_system_path(self):
        artifact_path = "/tmp/test"
        self.assertEqual(artifact_path, self.resolver.to_file_system_path(artifact_path))


class PluginFileNameResolverTests(TestCase):
    def setUp(self):
        super().setUp()
        self.resolver = supplier.PluginFileNameResolver("test-plugin")

    def test_resolve(self):
        self.resolver.revision = "abc123"
        self.assertEqual("test-plugin-abc123.zip", self.resolver.file_name)

    def test_artifact_key(self):
        self.assertEqual("test-plugin", self.resolver.artifact_key)

    def test_to_artifact_path(self):
        file_system_path = "/tmp/test"
        self.assertEqual(f"file://{file_system_path}", self.resolver.to_artifact_path(file_system_path))

    def test_to_file_system_path(self):
        file_system_path = "/tmp/test"
        self.assertEqual(file_system_path, self.resolver.to_file_system_path(f"file://{file_system_path}"))


class PruneTests(TestCase):
    LStat = collections.namedtuple("LStat", "st_ctime")

    @mock.patch("os.path.exists")
    @mock.patch("os.listdir")
    @mock.patch("os.path.isfile")
    @mock.patch("os.lstat")
    @mock.patch("os.remove")
    def test_does_not_touch_nonexisting_directory(self, rm, lstat, isfile, listdir, exists):
        exists.return_value = False

        supplier._prune(root_path="/tmp/test", max_age_days=7)

        self.assertEqual(0, listdir.call_count, "attempted to list a non-existing directory")

    @mock.patch("os.path.exists")
    @mock.patch("os.listdir")
    @mock.patch("os.path.isfile")
    @mock.patch("os.lstat")
    @mock.patch("os.remove")
    def test_prunes_old_files(self, rm, lstat, isfile, listdir, exists):
        exists.return_value = True
        listdir.return_value = ["opensearch-1.0.0.tar.gz", "some-subdir", "opensearch-7.3.0-darwin-x86_64.tar.gz"]
        isfile.side_effect = [True, False, True]

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        ten_days_ago = now - datetime.timedelta(days=10)
        one_day_ago = now - datetime.timedelta(days=1)

        lstat.side_effect = [
            # opensearch-1.0.0.tar.gz
            PruneTests.LStat(st_ctime=int(ten_days_ago.timestamp())),
            # opensearch-1.0.1-x64.tar.gz
            PruneTests.LStat(st_ctime=int(one_day_ago.timestamp()))
        ]

        supplier._prune(root_path="/tmp/test", max_age_days=7)

        rm.assert_called_with("/tmp/test/opensearch-1.0.0.tar.gz")


class OpenSearchSourceSupplierTests(TestCase):
    def test_no_build(self):
        provision_config_instance = provision_config.ProvisionConfigInstance("default", root_path=None, config_paths=[], variables={
            "clean_command": "./gradlew clean",
            "system.build_command": "./gradlew assemble"
        })
        renderer = supplier.TemplateRenderer(version=None)
        opensearch = supplier.OpenSearchSourceSupplier(revision="abc",
                                                  os_src_dir="/src",
                                                  remote_url="",
                                                  provision_config_instance=provision_config_instance,
                                                  builder=None,
                                                  template_renderer=renderer)
        opensearch.prepare()
        # nothing has happened (intentionally) because there is no builder

    def test_build(self):
        provision_config_instance = provision_config.ProvisionConfigInstance("default", root_path=None, config_paths=[], variables={
            "clean_command": "./gradlew clean",
            "system.build_command": "./gradlew assemble"
        })
        builder = mock.create_autospec(supplier.Builder)
        renderer = supplier.TemplateRenderer(version="abc")
        opensearch = supplier.OpenSearchSourceSupplier(revision="abc",
                                                  os_src_dir="/src",
                                                  remote_url="",
                                                  provision_config_instance=provision_config_instance,
                                                  builder=builder,
                                                  template_renderer=renderer)
        opensearch.prepare()

        builder.build.assert_called_once_with(["./gradlew clean", "./gradlew assemble"])

    def test_raises_error_on_missing_provision_config_instance_variable(self):
        provision_config_instance = provision_config.ProvisionConfigInstance("default", root_path=None, config_paths=[], variables={
            "clean_command": "./gradlew clean",
            # system.build_command is not defined
        })
        renderer = supplier.TemplateRenderer(version="abc")
        builder = mock.create_autospec(supplier.Builder)
        opensearch = supplier.OpenSearchSourceSupplier(revision="abc",
                                                  os_src_dir="/src",
                                                  remote_url="",
                                                  provision_config_instance=provision_config_instance,
                                                  builder=builder,
                                                  template_renderer=renderer)
        with self.assertRaisesRegex(exceptions.SystemSetupError,
                                    "ProvisionConfigInstance \"default\" requires config key \"system.build_command\""):
            opensearch.prepare()

        self.assertEqual(0, builder.build.call_count)

    @mock.patch("glob.glob", lambda p: ["opensearch.tar.gz"])
    def test_add_opensearch_binary(self):
        provision_config_instance = provision_config.ProvisionConfigInstance("default", root_path=None, config_paths=[], variables={
            "clean_command": "./gradlew clean",
            "system.build_command": "./gradlew assemble",
            "system.artifact_path_pattern": "distribution/archives/tar/build/distributions/*.tar.gz"
        })
        renderer = supplier.TemplateRenderer(version="abc")
        opensearch = supplier.OpenSearchSourceSupplier(revision="abc",
                                                  os_src_dir="/src",
                                                  remote_url="",
                                                  provision_config_instance=provision_config_instance,
                                                  builder=None,
                                                  template_renderer=renderer)
        binaries = {}
        opensearch.add(binaries=binaries)
        self.assertEqual(binaries, {"opensearch": "opensearch.tar.gz"})


class ExternalPluginSourceSupplierTests(TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.along_opensearch = None
        self.standalone = None

    def setUp(self):
        self.along_opensearch = supplier.ExternalPluginSourceSupplier(
            plugin=provision_config.PluginDescriptor("some-plugin", core_plugin=False),
                                                              revision="abc",
                                                              # built along-side OS
                                                              src_dir="/src",
                                                              src_config={
                                                                  "plugin.some-plugin.src.subdir": "opensearch-extra/some-plugin",
                                                                  "plugin.some-plugin.build.artifact.subdir": "plugin/build/distributions"
                                                              },
                                                              builder=None)

        self.standalone = supplier.ExternalPluginSourceSupplier(plugin=provision_config.PluginDescriptor("some-plugin", core_plugin=False),
                                                                revision="abc",
                                                                # built separately
                                                                src_dir=None,
                                                                src_config={
                                                                    "plugin.some-plugin.src.dir": "/Projects/src/some-plugin",
                                                                    "plugin.some-plugin.build.artifact.subdir": "build/distributions"
                                                                },
                                                                builder=None)

    def test_invalid_config_no_source(self):
        with self.assertRaisesRegex(exceptions.SystemSetupError,
                                    "Neither plugin.some-plugin.src.dir nor plugin.some-plugin.src.subdir are set for plugin some-plugin."):
            supplier.ExternalPluginSourceSupplier(plugin=provision_config.PluginDescriptor("some-plugin", core_plugin=False),
                                                  revision="abc",
                                                  # built separately
                                                  src_dir=None,
                                                  src_config={
                                                      # but no source config
                                                      # "plugin.some-plugin.src.dir": "/Projects/src/some-plugin",
                                                      "plugin.some-plugin.build.artifact.subdir": "build/distributions"
                                                  },
                                                  builder=None)

    def test_invalid_config_duplicate_source(self):
        with self.assertRaisesRegex(exceptions.SystemSetupError,
                                    "Can only specify one of plugin.duplicate.src.dir and plugin.duplicate.src.subdir but both are set."):
            supplier.ExternalPluginSourceSupplier(plugin=provision_config.PluginDescriptor("duplicate", core_plugin=False),
                                                  revision="abc",
                                                  src_dir=None,
                                                  src_config={
                                                      "plugin.duplicate.src.subdir": "opensearch-extra/some-plugin",
                                                      "plugin.duplicate.src.dir": "/Projects/src/some-plugin",
                                                      "plugin.duplicate.build.artifact.subdir": "build/distributions"
                                                  },
                                                  builder=None)

    def test_standalone_plugin_overrides_build_dir(self):
        self.assertEqual("/Projects/src/some-plugin", self.standalone.override_build_dir)

    def test_along_os_plugin_keeps_build_dir(self):
        self.assertIsNone(self.along_opensearch.override_build_dir)

    @mock.patch("glob.glob", lambda p: ["/src/opensearch-extra/some-plugin/plugin/build/distributions/some-plugin.zip"])
    def test_add_binary_built_along_opensearch(self):
        binaries = {}
        self.along_opensearch.add(binaries)
        self.assertDictEqual(binaries,
                             {"some-plugin": "file:///src/opensearch-extra/some-plugin/plugin/build/distributions/some-plugin.zip"})

    @mock.patch("glob.glob", lambda p: ["/Projects/src/some-plugin/build/distributions/some-plugin.zip"])
    def test_resolve_plugin_binary_built_standalone(self):
        binaries = {}
        self.along_opensearch.add(binaries)
        self.assertDictEqual(binaries,
                             {"some-plugin": "file:///Projects/src/some-plugin/build/distributions/some-plugin.zip"})


class CorePluginSourceSupplierTests(TestCase):
    @mock.patch("glob.glob", lambda p: ["/src/opensearch/core-plugin/build/distributions/core-plugin.zip"])
    def test_resolve_plugin_binary(self):
        s = supplier.CorePluginSourceSupplier(plugin=provision_config.PluginDescriptor("core-plugin", core_plugin=True),
                                              # built separately
                                              os_src_dir="/src/opensearch",
                                              builder=None)
        binaries = {}
        s.add(binaries)
        self.assertDictEqual(binaries, {"core-plugin": "file:///src/opensearch/core-plugin/build/distributions/core-plugin.zip"})


class PluginDistributionSupplierTests(TestCase):
    def test_resolve_plugin_url(self):
        v = {"plugin_logstash_release_url":
            "https://artifacts.opensearch.org/logstash/logstash-oss-with-opensearch-output-plugin-{{VERSION}}-linux-x64.tar.gz"}
        renderer = supplier.TemplateRenderer(version="7.13.2")
        s = supplier.PluginDistributionSupplier(repo=supplier.DistributionRepository(name="release",
                                                                                     distribution_config=v,
                                                                                     template_renderer=renderer),
                                                plugin=provision_config.PluginDescriptor("logstash"))
        binaries = {}
        s.add(binaries)
        self.assertDictEqual(
            binaries, {"logstash":
                "https://artifacts.opensearch.org/logstash/logstash-oss-with-opensearch-output-plugin-7.13.2-linux-x64.tar.gz"})


class CreateSupplierTests(TestCase):
    def test_derive_supply_requirements_os_source_build(self):
        # corresponds to --revision="abc"
        requirements = supplier._supply_requirements(
            sources=True, distribution=False, plugins=[], revisions={"opensearch": "abc"}, distribution_version=None)
        self.assertDictEqual({"opensearch": ("source", "abc", True)}, requirements)

    def test_derive_supply_requirements_os_distribution(self):
        # corresponds to --distribution-version=1.0.0
        requirements = supplier._supply_requirements(
            sources=False, distribution=True, plugins=[], revisions={}, distribution_version="1.0.0")
        self.assertDictEqual({"opensearch": ("distribution", "1.0.0", False)}, requirements)

    def test_derive_supply_requirements_os_and_plugin_source_build(self):
        # corresponds to --revision="opensearch:abc,community-plugin:effab"
        core_plugin = provision_config.PluginDescriptor("analysis-icu", core_plugin=True)
        external_plugin = provision_config.PluginDescriptor("community-plugin", core_plugin=False)

        requirements = supplier._supply_requirements(sources=True, distribution=False, plugins=[core_plugin, external_plugin],
                                                     revisions={"opensearch": "abc", "all": "abc", "community-plugin": "effab"},
                                                     distribution_version=None)
        self.assertDictEqual({
            "opensearch": ("source", "abc", True),
            # core plugin configuration is forced to be derived from OS
            "analysis-icu": ("source", "abc", True),
            "community-plugin": ("source", "effab", True),
        }, requirements)

    def test_derive_supply_requirements_os_distribution_and_plugin_source_build(self):
        # corresponds to --revision="community-plugin:effab" --distribution-version="1.0.0"
        core_plugin = provision_config.PluginDescriptor("analysis-icu", core_plugin=True)
        external_plugin = provision_config.PluginDescriptor("community-plugin", core_plugin=False)

        requirements = supplier._supply_requirements(sources=False, distribution=True, plugins=[core_plugin, external_plugin],
                                                     revisions={"community-plugin": "effab"},
                                                     distribution_version="1.0.0")
        # core plugin is not contained, its configured is forced to be derived by OS
        self.assertDictEqual({
            "opensearch": ("distribution", "1.0.0", False),
            # core plugin configuration is forced to be derived from OS
            "analysis-icu": ("distribution", "1.0.0", False),
            "community-plugin": ("source", "effab", True),
        }, requirements)

    def test_create_suppliers_for_os_only_config(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "builder", "distribution.version", "1.0.0")
        # default value from command line
        cfg.add(config.Scope.application, "builder", "source.revision", "current")
        cfg.add(config.Scope.application, "builder", "distribution.repository", "release")
        cfg.add(config.Scope.application, "distributions", "release.url",
                "https://artifacts.opensearch.org/releases/bundle/opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz")
        cfg.add(config.Scope.application, "distributions", "release.cache", True)
        cfg.add(config.Scope.application, "node", "root.dir", "/opt/benchmark")

        provision_config_instance = provision_config.ProvisionConfigInstance("default", root_path=None, config_paths=[])

        composite_supplier = supplier.create(cfg, sources=False, distribution=True, provision_config_instance=provision_config_instance)

        self.assertEqual(1, len(composite_supplier.suppliers))
        self.assertIsInstance(composite_supplier.suppliers[0], supplier.OpenSearchDistributionSupplier)

    @mock.patch("osbenchmark.utils.jvm.resolve_path", lambda v: (v, "/opt/java/java{}".format(v)))
    def test_create_suppliers_for_os_distribution_plugin_source_build(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "builder", "distribution.version", "1.0.0")
        # default value from command line
        cfg.add(config.Scope.application, "builder", "source.revision", "community-plugin:current")
        cfg.add(config.Scope.application, "builder", "distribution.repository", "release")
        cfg.add(config.Scope.application, "distributions", "release.url",
                "https://artifacts.opensearch.org/releases/bundle/opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz")
        cfg.add(config.Scope.application, "distributions", "release.cache", True)
        cfg.add(config.Scope.application, "node", "root.dir", "/opt/benchmark")
        cfg.add(config.Scope.application, "node", "src.root.dir", "/opt/benchmark/src")
        cfg.add(config.Scope.application, "source", "opensearch.src.subdir", "opensearch")
        cfg.add(config.Scope.application, "source", "plugin.community-plugin.src.dir", "/home/user/Projects/community-plugin")

        provision_config_instance = provision_config.ProvisionConfigInstance(
            "default", root_path=None, config_paths=[],
            variables={"build.jdk": "10"})
        core_plugin = provision_config.PluginDescriptor("analysis-icu", core_plugin=True)
        external_plugin = provision_config.PluginDescriptor("community-plugin", core_plugin=False)

        # --revision="community-plugin:effab" --distribution-version="1.0.0"
        composite_supplier = supplier.create(
            cfg, sources=False, distribution=True,
            provision_config_instance=provision_config_instance, plugins=[
            core_plugin,
            external_plugin
        ])

        self.assertEqual(3, len(composite_supplier.suppliers))
        self.assertIsInstance(composite_supplier.suppliers[0], supplier.OpenSearchDistributionSupplier)
        self.assertIsInstance(composite_supplier.suppliers[1], supplier.PluginDistributionSupplier)
        self.assertEqual(core_plugin, composite_supplier.suppliers[1].plugin)
        self.assertIsInstance(composite_supplier.suppliers[2].source_supplier, supplier.ExternalPluginSourceSupplier)
        self.assertEqual(external_plugin, composite_supplier.suppliers[2].source_supplier.plugin)
        self.assertIsNotNone(composite_supplier.suppliers[2].source_supplier.builder)

    @mock.patch("osbenchmark.utils.jvm.resolve_path", lambda v: (v, "/opt/java/java{}".format(v)))
    def test_create_suppliers_for_os_and_plugin_source_build(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "builder", "source.revision", "opensearch:abc,community-plugin:current")
        cfg.add(config.Scope.application, "builder", "distribution.repository", "release")
        cfg.add(config.Scope.application, "distributions", "release.url",
                "https://artifacts.opensearch.org/releases/bundle/opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz")
        cfg.add(config.Scope.application, "distributions", "release.cache", True)
        cfg.add(config.Scope.application, "node", "root.dir", "/opt/benchmark")
        cfg.add(config.Scope.application, "node", "src.root.dir", "/opt/benchmark/src")
        cfg.add(config.Scope.application, "source", "opensearch.src.subdir", "opensearch")
        cfg.add(config.Scope.application, "source", "remote.repo.url", "https://github.com/opensearch-project/OpenSearch.git")
        cfg.add(config.Scope.application, "source", "plugin.community-plugin.src.subdir", "opensearch-extra/community-plugin")

        provision_config_instance = provision_config.ProvisionConfigInstance("default", root_path=None, config_paths=[], variables={
            "clean_command": "./gradlew clean",
            "build_command": "./gradlew assemble",
            "build.jdk": "11"
        })
        core_plugin = provision_config.PluginDescriptor("analysis-icu", core_plugin=True)
        external_plugin = provision_config.PluginDescriptor("community-plugin", core_plugin=False)

        # --revision="opensearch:abc,community-plugin:effab"
        composite_supplier = supplier.create(
            cfg, sources=True, distribution=False,
            provision_config_instance=provision_config_instance, plugins=[
            core_plugin,
            external_plugin
        ])

        self.assertEqual(3, len(composite_supplier.suppliers))
        self.assertIsInstance(composite_supplier.suppliers[0].source_supplier, supplier.OpenSearchSourceSupplier)
        self.assertIsInstance(composite_supplier.suppliers[1].source_supplier, supplier.CorePluginSourceSupplier)
        self.assertEqual(core_plugin, composite_supplier.suppliers[1].source_supplier.plugin)
        self.assertIsInstance(composite_supplier.suppliers[2].source_supplier, supplier.ExternalPluginSourceSupplier)
        self.assertEqual(external_plugin, composite_supplier.suppliers[2].source_supplier.plugin)
        self.assertIsNotNone(composite_supplier.suppliers[2].source_supplier.builder)


class DistributionRepositoryTests(TestCase):
    @mock.patch("osbenchmark.utils.sysstats.os_name", return_value="Linux")
    @mock.patch("osbenchmark.utils.sysstats.cpu_arch", return_value="x64")
    def test_release_repo_config_with_default_url(self, os_name, cpu_arch):
        renderer = supplier.TemplateRenderer(version="1.0.0")
        repo = supplier.DistributionRepository(name="release", distribution_config={
            "runtime.jdk.bundled": "true",
            "jdk.bundled.release_url":
                "https://artifacts.opensearch.org/releases/bundle/opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz",
            "release.cache": "true"
        }, template_renderer=renderer)
        self.assertEqual("https://artifacts.opensearch.org/releases/bundle/opensearch/1.0.0/opensearch-1.0.0-linux-x64.tar.gz",
         repo.download_url)
        self.assertEqual("opensearch-1.0.0-linux-x64.tar.gz", repo.file_name)
        self.assertTrue(repo.cache)

    @mock.patch("osbenchmark.utils.sysstats.cpu_arch", return_value="x64")
    def test_release_repo_config_with_user_url(self, cpu_arch):
        renderer = supplier.TemplateRenderer(version="1.0.0")
        repo = supplier.DistributionRepository(name="release", distribution_config={
            "jdk.unbundled.release_url":
                "https://artifacts.opensearch.org/releases/bundle/opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz",
            "runtime.jdk.bundled": "false",
            # user override
            "release.url":
                "https://artifacts.opensearch.org/releases/bundle/opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz",
            "release.cache": "false"
        }, template_renderer=renderer)
        self.assertEqual("https://artifacts.opensearch.org/releases/bundle/opensearch/1.0.0/opensearch-1.0.0-linux-x64.tar.gz",
         repo.download_url)
        self.assertEqual("opensearch-1.0.0-linux-x64.tar.gz", repo.file_name)
        self.assertFalse(repo.cache)

    def test_missing_url(self):
        renderer = supplier.TemplateRenderer(version="1.0.0")
        repo = supplier.DistributionRepository(name="miss", distribution_config={
            "jdk.unbundled.release_url": "https://artifacts.opensearch\
                .org/releases/bundle/opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz",
            "runtime.jdk.bundled": "false",
            "release.cache": "true"
        }, template_renderer=renderer)
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            # pylint: disable=pointless-statement
            # noinspection PyStatementEffect
            repo.download_url
        self.assertEqual("Neither config key [miss.url] nor [jdk.unbundled.miss_url] is defined.", ctx.exception.args[0])

    def test_missing_cache(self):
        renderer = supplier.TemplateRenderer(version="1.0.0")
        repo = supplier.DistributionRepository(name="release", distribution_config={
            "jdk.unbundled.release.url": "https://artifacts.opensearch\
                .org/releases/bundle/opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz",
            "runtime.jdk.bundled": "false"
        }, template_renderer=renderer)
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            # pylint: disable=pointless-statement
            # noinspection PyStatementEffect
            repo.cache
        self.assertEqual("Mandatory config key [release.cache] is undefined.", ctx.exception.args[0])

    def test_invalid_cache_value(self):
        renderer = supplier.TemplateRenderer(version="1.0.0")
        repo = supplier.DistributionRepository(name="release", distribution_config={
            "jdk.unbundled.release.url": "https://artifacts.opensearch\
                .org/releases/bundle/opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz",
            "runtime.jdk.bundled": "false",
            "release.cache": "Invalid"
        }, template_renderer=renderer)
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            # pylint: disable=pointless-statement
            # noinspection PyStatementEffect
            repo.cache
        self.assertEqual("Value [Invalid] for config key [release.cache] is not a valid boolean value.", ctx.exception.args[0])

    def test_plugin_config_with_default_url(self):
        renderer = supplier.TemplateRenderer(version="5.5.0")
        repo = supplier.DistributionRepository(name="release", distribution_config={
            "runtime.jdk.bundled": "false",
            "plugin_example_release_url": "https://artifacts.example.org/downloads/plugins/example-{{VERSION}}.zip"
        }, template_renderer=renderer)
        self.assertEqual("https://artifacts.example.org/downloads/plugins/example-5.5.0.zip", repo.plugin_download_url("example"))

    def test_plugin_config_with_user_url(self):
        renderer = supplier.TemplateRenderer(version="5.5.0")
        repo = supplier.DistributionRepository(name="release", distribution_config={
            "runtime.jdk.bundled": "false",
            "plugin_example_release_url": "https://artifacts.example.org/downloads/plugins/example-{{VERSION}}.zip",
            # user override
            "plugin.example.release.url": "https://mirror.example.org/downloads/plugins/example-{{VERSION}}.zip"
        }, template_renderer=renderer)
        self.assertEqual("https://mirror.example.org/downloads/plugins/example-5.5.0.zip", repo.plugin_download_url("example"))

    def test_missing_plugin_config(self):
        renderer = supplier.TemplateRenderer(version="5.5.0")
        repo = supplier.DistributionRepository(name="release", distribution_config={
            "runtime.jdk.bundled": "false",
        }, template_renderer=renderer)
        self.assertIsNone(repo.plugin_download_url("not existing"))
