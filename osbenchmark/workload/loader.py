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
import glob
import json
import logging
import os
import re
import sys
import tempfile
import urllib.error

import jinja2
import jinja2.exceptions
import jsonschema
import tabulate
from jinja2 import meta, select_autoescape

from osbenchmark import exceptions, time, PROGRAM_NAME, config, version
from osbenchmark.workload import params, workload
from osbenchmark.utils import io, collections, convert, net, console, modules, opts, repo


class WorkloadSyntaxError(exceptions.InvalidSyntax):
    """
    Raised whenever a syntax problem is encountered when loading the workload specification.
    """


class WorkloadProcessor:
    def on_after_load_workload(self, workload):
        """
        This method is called by Benchmark after a workload has been loaded. Implementations are expected to modify the
        provided workload object in place.

        :param workload: The current workload.
        """

    def on_prepare_workload(self, workload, data_root_dir):
        """
        This method is called by Benchmark after the "after_load_workload" phase. Here, any data that is necessary for
        benchmark execution should be prepared, e.g. by downloading data or generating it. Implementations should
        be aware that this method might be called on a different machine than "on_after_load_workload" and they cannot
        share any state in between phases.

        :param workload: The current workload. This parameter should be treated as effectively immutable. Any modifications
                      will not be reflected in subsequent phases of the benchmark.
        :param data_root_dir: The data root directory on the current machine as configured by the user.
        :return: an Iterable[Callable, dict] of function/parameter pairs to be executed by the prepare workload's executor
        actors.
        """
        return []


class WorkloadProcessorRegistry:
    def __init__(self, cfg):
        self.required_processors = [TaskFilterWorkloadProcessor(cfg), TestModeWorkloadProcessor(cfg)]
        self.workload_processors = []
        self.offline = cfg.opts("system", "offline.mode")
        self.test_mode = cfg.opts("workload", "test.mode.enabled", mandatory=False, default_value=False)
        self.base_config = cfg
        self.custom_configuration = False

    def register_workload_processor(self, processor):
        if not self.custom_configuration:
            # given processor should become the only element
            self.workload_processors = []
        if not isinstance(processor, DefaultWorkloadPreparator):
            # stop resetting self.workload_processors
            self.custom_configuration = True
        if hasattr(processor, "downloader"):
            processor.downloader = Downloader(self.offline, self.test_mode)
        if hasattr(processor, "decompressor"):
            processor.decompressor = Decompressor()
        self.workload_processors.append(processor)

    @property
    def processors(self):
        if not self.custom_configuration:
            self.register_workload_processor(DefaultWorkloadPreparator(self.base_config))
        return [*self.required_processors, *self.workload_processors]


def workloads(cfg):
    """

    Lists all known workloads. Note that users can specify a distribution version so if different workloads are available for
    different versions, this will be reflected in the output.

    :param cfg: The config object.
    :return: A list of workloads that are available for the provided distribution version or else for the master version.
    """
    repo = workload_repo(cfg)
    return [_load_single_workload(cfg, repo, workload_name) for workload_name in repo.workload_names]


def list_workloads(cfg):
    available_workloads = workloads(cfg)
    only_auto_generated_test_procedures = all(t.default_test_procedure.auto_generated for t in available_workloads)

    data = []
    for t in available_workloads:
        line = [t.name, t.description, convert.number_to_human_string(t.number_of_documents),
                convert.bytes_to_human_string(t.compressed_size_in_bytes),
                convert.bytes_to_human_string(t.uncompressed_size_in_bytes)]
        if not only_auto_generated_test_procedures:
            line.append(t.default_test_procedure)
            line.append(",".join(map(str, t.test_procedures)))
        data.append(line)

    headers = ["Name", "Description", "Documents", "Compressed Size", "Uncompressed Size"]
    if not only_auto_generated_test_procedures:
        headers.append("Default TestProcedure")
        headers.append("All TestProcedures")

    console.println("Available workloads:\n")
    console.println(tabulate.tabulate(tabular_data=data, headers=headers))


def workload_info(cfg):
    def format_task(t, indent="", num="", suffix=""):
        msg = "{}{}{}".format(indent, num, str(t))
        if t.clients > 1:
            msg += " ({} clients)".format(t.clients)
        msg += suffix
        return msg

    def test_procedure_info(c):
        if not c.auto_generated:
            msg = "TestProcedure [{}]".format(c.name)
            if c.default:
                msg += " (run by default)"
            console.println(msg, underline="=", overline="=")
            if c.description:
                console.println("\n{}".format(c.description))

        console.println("\nSchedule:", underline="-")
        console.println("")
        for num, task in enumerate(c.schedule, start=1):
            if task.nested:
                console.println(format_task(task, suffix=":", num="{}. ".format(num)))
                for leaf_num, leaf_task in enumerate(task, start=1):
                    console.println(format_task(leaf_task, indent="\t", num="{}.{} ".format(num, leaf_num)))
            else:
                console.println(format_task(task, num="{}. ".format(num)))

    t = load_workload(cfg)
    console.println("Showing details for workload [{}]:\n".format(t.name))
    console.println("* Description: {}".format(t.description))
    if t.number_of_documents:
        console.println("* Documents: {}".format(convert.number_to_human_string(t.number_of_documents)))
        console.println("* Compressed Size: {}".format(convert.bytes_to_human_string(t.compressed_size_in_bytes)))
        console.println("* Uncompressed Size: {}".format(convert.bytes_to_human_string(t.uncompressed_size_in_bytes)))
    console.println("")

    if t.selected_test_procedure:
        test_procedure_info(t.selected_test_procedure)
    else:
        for test_procedure in t.test_procedures:
            test_procedure_info(test_procedure)
            console.println("")


def load_workload(cfg):
    """

    Loads a workload

    :param cfg: The config object. It contains the name of the workload to load.
    :return: The loaded workload.
    """
    repo = workload_repo(cfg)
    return _load_single_workload(cfg, repo, repo.workload_name)


def _load_single_workload(cfg, workload_repository, workload_name):
    try:
        workload_dir = workload_repository.workload_dir(workload_name)
        reader = WorkloadFileReader(cfg)
        current_workload = reader.read(workload_name, workload_repository.workload_file(workload_name), workload_dir)
        tpr = WorkloadProcessorRegistry(cfg)
        has_plugins = load_workload_plugins(cfg, workload_name, register_workload_processor=tpr.register_workload_processor)
        current_workload.has_plugins = has_plugins
        for processor in tpr.processors:
            processor.on_after_load_workload(current_workload)
        return current_workload
    except FileNotFoundError as e:
        logging.getLogger(__name__).exception("Cannot load workload [%s]", workload_name)
        raise exceptions.SystemSetupError(f"Cannot load workload [{workload_name}]. "
                                          f"List the available workloads with [{PROGRAM_NAME} list workloads].") from e
    except BaseException:
        logging.getLogger(__name__).exception("Cannot load workload [%s]", workload_name)
        raise


def load_workload_plugins(cfg,
                       workload_name,
                       register_runner=None,
                       register_scheduler=None,
                       register_workload_processor=None,
                       force_update=False):
    """
    Loads plugins that are defined for the current workload (as specified by the configuration).

    :param cfg: The config object.
    :param workload_name: Name of the workload for which plugins should be loaded.
    :param register_runner: An optional function where runners can be registered.
    :param register_scheduler: An optional function where custom schedulers can be registered.
    :param register_workload_processor: An optional function where workload processors can be registered.
    :param force_update: If set to ``True`` this ensures that the workload is first updated from the remote repository.
                         Defaults to ``False``.
    :return: True iff this workload defines plugins and they have been loaded.
    """
    repo = workload_repo(cfg, fetch=force_update, update=force_update)
    workload_plugin_path = repo.workload_dir(workload_name)
    logging.getLogger(__name__).debug("Invoking plugin_reader with name [%s] resolved to path [%s]", workload_name, workload_plugin_path)
    plugin_reader = WorkloadPluginReader(workload_plugin_path, register_runner, register_scheduler, register_workload_processor)

    if plugin_reader.can_load():
        plugin_reader.load()
        return True
    else:
        return False


def set_absolute_data_path(cfg, t):
    """
    Sets an absolute data path on all document files in this workload.
    Internally we store only relative paths in the workload as long as possible
    as the data root directory may be different on each host. In the end we need to have an absolute path though when we want to read the
    file on the target host.

    :param cfg: The config object.
    :param t: The workload to modify.
    """

    def first_existing(root_dirs, f):
        for root_dir in root_dirs:
            p = os.path.join(root_dir, f)
            if os.path.exists(p):
                return p
        return None

    for corpus in t.corpora:
        data_root = data_dir(cfg, t.name, corpus.name)
        for document_set in corpus.documents:
            # At this point we can assume that the file is available locally. Check which path exists and set it.
            if document_set.document_archive:
                document_set.document_archive = first_existing(data_root, document_set.document_archive)
            if document_set.document_file:
                document_set.document_file = first_existing(data_root, document_set.document_file)


def is_simple_workload_mode(cfg):
    return cfg.exists("workload", "workload.path")


def workload_path(cfg):
    repo = workload_repo(cfg)
    workload_name = repo.workload_name
    workload_dir = repo.workload_dir(workload_name)
    return workload_dir


def workload_repo(cfg, fetch=True, update=True):
    if is_simple_workload_mode(cfg):
        workload_path = cfg.opts("workload", "workload.path")
        return SimpleWorkloadRepository(workload_path)
    else:
        return GitWorkloadRepository(cfg, fetch, update)


def data_dir(cfg, workload_name, corpus_name):
    """
    Determines potential data directories for the provided workload and corpus name.

    :param cfg: The config object.
    :param workload_name: Name of the current workload.
    :param corpus_name: Name of the current corpus.
    :return: A list containing either one or two elements. Each element contains a path to a directory which may contain document files.
    """
    corpus_dir = os.path.join(cfg.opts("benchmarks", "local.dataset.cache"), corpus_name)
    if is_simple_workload_mode(cfg):
        workload_path = cfg.opts("workload", "workload.path")
        r = SimpleWorkloadRepository(workload_path)
        # data should always be stored in the workload's directory. If the user uses the same directory on all machines this will even work
        # in the distributed case. However, the user is responsible to ensure that this is actually the case.
        return [r.workload_dir(workload_name), corpus_dir]
    else:
        return [corpus_dir]


class GitWorkloadRepository:
    def __init__(self, cfg, fetch, update, repo_class=repo.BenchmarkRepository):
        # current workload name (if any)
        self.workload_name = cfg.opts("workload", "workload.name", mandatory=False)
        distribution_version = cfg.opts("builder", "distribution.version", mandatory=False)
        repo_name = cfg.opts("workload", "repository.name")
        repo_revision = cfg.opts("workload", "repository.revision", mandatory=False)
        offline = cfg.opts("system", "offline.mode")
        remote_url = cfg.opts("workloads", "%s.url" % repo_name, mandatory=False)
        root = cfg.opts("node", "root.dir")
        workload_repositories = cfg.opts("benchmarks", "workload.repository.dir")
        workloads_dir = os.path.join(root, workload_repositories)

        self.repo = repo_class(remote_url, workloads_dir, repo_name, "workloads", offline, fetch)
        if update:
            if repo_revision:
                self.repo.checkout(repo_revision)
            else:
                self.repo.update(distribution_version)
                cfg.add(config.Scope.applicationOverride, "workload", "repository.revision", self.repo.revision)

    @property
    def workload_names(self):
        return filter(lambda p: os.path.exists(self.workload_file(p)), next(os.walk(self.repo.repo_dir))[1])

    def workload_dir(self, workload_name):
        return os.path.join(self.repo.repo_dir, workload_name)

    def workload_file(self, workload_name):
        return os.path.join(self.workload_dir(workload_name), "workload.json")


class SimpleWorkloadRepository:
    def __init__(self, workload_path):
        if not os.path.exists(workload_path):
            raise exceptions.SystemSetupError("Workload path %s does not exist" % workload_path)

        if os.path.isdir(workload_path):
            self.workload_name = io.basename(workload_path)
            self._workload_dir = workload_path
            self._workload_file = os.path.join(workload_path, "workload.json")
            if not os.path.exists(self._workload_file):
                raise exceptions.SystemSetupError("Could not find workload.json in %s" % workload_path)
        elif os.path.isfile(workload_path):
            if io.has_extension(workload_path, ".json"):
                self._workload_dir = io.dirname(workload_path)
                self._workload_file = workload_path
                self.workload_name = io.splitext(io.basename(workload_path))[0]
            else:
                raise exceptions.SystemSetupError("%s has to be a JSON file" % workload_path)
        else:
            raise exceptions.SystemSetupError("%s is neither a file nor a directory" % workload_path)

    @property
    def workload_names(self):
        return [self.workload_name]

    def workload_dir(self, workload_name):
        assert workload_name == self.workload_name, "Expect provided workload name [%s] to match [%s]" % (workload_name, self.workload_name)
        return self._workload_dir

    def workload_file(self, workload_name):
        assert workload_name == self.workload_name
        return self._workload_file


def operation_parameters(t, task):
    op = task.operation
    if op.param_source:
        return params.param_source_for_name(op.param_source, t, op.params)
    else:
        return params.param_source_for_operation(op.type, t, op.params, task.name)


def used_corpora(t):
    corpora = {}
    if t.corpora:
        test_procedure = t.selected_test_procedure_or_default
        for task in test_procedure.schedule:
            for sub_task in task:
                param_source = operation_parameters(t, sub_task)
                if hasattr(param_source, "corpora"):
                    for c in param_source.corpora:
                        # We might have the same corpus *but* they contain different doc sets. Therefore also need to union over doc sets.
                        corpora[c.name] = corpora.get(c.name, c).union(c)
    return corpora.values()


class DefaultWorkloadPreparator(WorkloadProcessor):
    def __init__(self, cfg):
        super().__init__()
        self.cfg = cfg
        # just declare here, will be injected later
        self.downloader = None
        self.decompressor = None
        self.workload = None

    @staticmethod
    def prepare_docs(cfg, workload, corpus, preparator):
        for document_set in corpus.documents:
            if document_set.is_bulk:
                data_root = data_dir(cfg, workload.name, corpus.name)
                logging.getLogger(__name__).info("Resolved data root directory for document corpus [%s] in workload [%s] "
                                                 "to [%s].", corpus.name, workload.name, data_root)
                if len(data_root) == 1:
                    preparator.prepare_document_set(document_set, data_root[0])
                # attempt to prepare everything in the current directory and fallback to the corpus directory
                elif not preparator.prepare_bundled_document_set(document_set, data_root[0]):
                    preparator.prepare_document_set(document_set, data_root[1])

    def on_prepare_workload(self, workload, data_root_dir):
        prep = DocumentSetPreparator(workload.name, self.downloader, self.decompressor)
        for corpus in used_corpora(workload):
            params = {
                "cfg": self.cfg,
                "workload": workload,
                "corpus": corpus,
                "preparator": prep
            }
            yield DefaultWorkloadPreparator.prepare_docs, params


class Decompressor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def decompress(self, archive_path, documents_path, uncompressed_size):
        if uncompressed_size:
            msg = f"Decompressing workload data from [{archive_path}] to [{documents_path}] (resulting size: " \
                  f"[{convert.bytes_to_gb(uncompressed_size):.2f}] GB) ... "
        else:
            msg = f"Decompressing workload data from [{archive_path}] to [{documents_path}] ... "

        console.info(msg, end="", flush=True, logger=self.logger)
        io.decompress(archive_path, io.dirname(archive_path))
        console.println("[OK]")
        if not os.path.isfile(documents_path):
            raise exceptions.DataError(
                f"Decompressing [{archive_path}] did not create [{documents_path}]. Please check with the workload "
                f"author if the compressed archive has been created correctly.")

        extracted_bytes = os.path.getsize(documents_path)
        if uncompressed_size is not None and extracted_bytes != uncompressed_size:
            raise exceptions.DataError(f"[{documents_path}] is corrupt. Extracted [{extracted_bytes}] bytes "
                                       f"but [{uncompressed_size}] bytes are expected.")


class Downloader:
    def __init__(self, offline, test_mode):
        self.offline = offline
        self.test_mode = test_mode
        self.logger = logging.getLogger(__name__)

    def download(self, base_url, target_path, size_in_bytes):
        file_name = os.path.basename(target_path)

        if not base_url:
            raise exceptions.DataError("Cannot download data because no base URL is provided.")
        if self.offline:
            raise exceptions.SystemSetupError(f"Cannot find [{target_path}]. Please disable offline mode and retry.")

        if base_url.endswith("/"):
            separator = ""
        else:
            separator = "/"
        # join manually as `urllib.parse.urljoin` does not work with S3 or GS URL schemes.
        data_url = f"{base_url}{separator}{file_name}"
        try:
            io.ensure_dir(os.path.dirname(target_path))
            if size_in_bytes:
                size_in_mb = round(convert.bytes_to_mb(size_in_bytes))
                self.logger.info("Downloading data from [%s] (%s MB) to [%s].", data_url, size_in_mb, target_path)
            else:
                self.logger.info("Downloading data from [%s] to [%s].", data_url, target_path)

            # we want to have a bit more accurate download progress as these files are typically very large
            progress = net.Progress("[INFO] Downloading workload data", accuracy=1)
            net.download(data_url, target_path, size_in_bytes, progress_indicator=progress)
            progress.finish()
            self.logger.info("Downloaded data from [%s] to [%s].", data_url, target_path)
        except urllib.error.HTTPError as e:
            if e.code == 404 and self.test_mode:
                raise exceptions.DataError("This workload does not support test mode. Ask the workload author to add it or"
                                           " disable test mode and retry.") from None
            else:
                msg = f"Could not download [{data_url}] to [{target_path}]"
                if e.reason:
                    msg += f" (HTTP status: {e.code}, reason: {e.reason})"
                else:
                    msg += f" (HTTP status: {e.code})"
                raise exceptions.DataError(msg) from e
        except urllib.error.URLError as e:
            raise exceptions.DataError(f"Could not download [{data_url}] to [{target_path}].") from e

        if not os.path.isfile(target_path):
            raise exceptions.SystemSetupError(f"Could not download [{data_url}] to [{target_path}]. Verify data "
                                              f"are available at [{data_url}] and check your Internet connection.")

        actual_size = os.path.getsize(target_path)
        if size_in_bytes is not None and actual_size != size_in_bytes:
            raise exceptions.DataError(f"[{target_path}] is corrupt. Downloaded [{actual_size}] bytes "
                                       f"but [{size_in_bytes}] bytes are expected.")


class DocumentSetPreparator:
    def __init__(self, workload_name, downloader, decompressor):
        self.workload_name = workload_name
        self.downloader = downloader
        self.decompressor = decompressor

    def is_locally_available(self, file_name):
        return os.path.isfile(file_name)

    def has_expected_size(self, file_name, expected_size):
        return expected_size is None or os.path.getsize(file_name) == expected_size

    def create_file_offset_table(self, document_file_path, expected_number_of_lines):
        # just rebuild the file every time for the time being. Later on, we might check the data file fingerprint to avoid it
        lines_read = io.prepare_file_offset_table(document_file_path)
        if lines_read and lines_read != expected_number_of_lines:
            io.remove_file_offset_table(document_file_path)
            raise exceptions.DataError(f"Data in [{document_file_path}] for workload [{self.workload_name}] are invalid. "
                                       f"Expected [{expected_number_of_lines}] lines but got [{lines_read}].")

    def prepare_document_set(self, document_set, data_root):
        """
        Prepares a document set locally.

        Precondition: The document set contains either a compressed or an uncompressed document file reference.
        Postcondition: Either following files will be present locally:

            * The compressed document file (if specified originally in the corpus)
            * The uncompressed document file
            * A file offset table based on the document file

            Or this method will raise an appropriate Exception (download error, inappropriate specification of files, ...).

        :param document_set: A document set.
        :param data_root: The data root directory for this document set.
        """
        doc_path = os.path.join(data_root, document_set.document_file)
        archive_path = os.path.join(data_root, document_set.document_archive) if document_set.has_compressed_corpus() else None
        while True:
            if self.is_locally_available(doc_path) and \
                    self.has_expected_size(doc_path, document_set.uncompressed_size_in_bytes):
                break
            if document_set.has_compressed_corpus() and \
                    self.is_locally_available(archive_path) and \
                    self.has_expected_size(archive_path, document_set.compressed_size_in_bytes):
                self.decompressor.decompress(archive_path, doc_path, document_set.uncompressed_size_in_bytes)
            else:
                if document_set.has_compressed_corpus():
                    target_path = archive_path
                    expected_size = document_set.compressed_size_in_bytes
                elif document_set.has_uncompressed_corpus():
                    target_path = doc_path
                    expected_size = document_set.uncompressed_size_in_bytes
                else:
                    # this should not happen in practice as the JSON schema should take care of this
                    raise exceptions.BenchmarkAssertionError(f"Workload {self.workload_name} specifies documents but no corpus")

                try:
                    self.downloader.download(document_set.base_url, target_path, expected_size)
                except exceptions.DataError as e:
                    if e.message == "Cannot download data because no base URL is provided." and \
                       self.is_locally_available(target_path):
                        raise exceptions.DataError(f"[{target_path}] is present but does not have the expected "
                                                   f"size of [{expected_size}] bytes and it cannot be downloaded "
                                                   f"because no base URL is provided.") from None
                    else:
                        raise

        self.create_file_offset_table(doc_path, document_set.number_of_lines)

    def prepare_bundled_document_set(self, document_set, data_root):
        """
        Prepares a document set that comes "bundled" with the workload, i.e. the data files are in the same directory as the workload.
        This is a "lightweight" version of #prepare_document_set() which assumes that at least one file is already present in the
        current directory. It will attempt to find the appropriate files, decompress if necessary and create a file offset table.

        Precondition: The document set contains either a compressed or an uncompressed document file reference.
        Postcondition: If this method returns ``True``, the following files will be present locally:

            * The compressed document file (if specified originally in the corpus)
            * The uncompressed document file
            * A file offset table based on the document file

        If this method returns ``False`` either the document size is wrong or any files have not been found.

        :param document_set: A document set.
        :param data_root: The data root directory for this document set (should be the same as the workload file).
        :return: See postcondition.
        """
        doc_path = os.path.join(data_root, document_set.document_file)
        archive_path = os.path.join(data_root, document_set.document_archive) if document_set.has_compressed_corpus() else None

        while True:
            if self.is_locally_available(doc_path):
                if self.has_expected_size(doc_path, document_set.uncompressed_size_in_bytes):
                    self.create_file_offset_table(doc_path, document_set.number_of_lines)
                    return True
                else:
                    raise exceptions.DataError(f"[{doc_path}] is present but does not have the expected size "
                                               f"of [{document_set.uncompressed_size_in_bytes}] bytes.")

            if document_set.has_compressed_corpus() and self.is_locally_available(archive_path):
                if self.has_expected_size(archive_path, document_set.compressed_size_in_bytes):
                    self.decompressor.decompress(archive_path, doc_path, document_set.uncompressed_size_in_bytes)
                else:
                    # treat this is an error because if the file is present but the size does not match, something is
                    # really fishy. It is likely that the user is currently creating a new workload and did not specify
                    # the file size correctly.
                    raise exceptions.DataError(f"[{archive_path}] is present but does not have "
                                               f"the expected size of [{document_set.compressed_size_in_bytes}] bytes.")
            else:
                return False


class TemplateSource:
    """
    Prepares the fully assembled workload file from file or string.
    Doesn't render using jinja2, but embeds workload fragments referenced with
    benchmark.collect(parts=...
    """

    collect_parts_re = re.compile(r"{{\ +?benchmark\.collect\(parts=\"(.+?(?=\"))\"\)\ +?}}")

    def __init__(self, base_path, template_file_name, source=io.FileSource, fileglobber=glob.glob):
        self.base_path = base_path
        self.template_file_name = template_file_name
        self.source = source
        self.fileglobber = fileglobber
        self.assembled_source = None
        self.logger = logging.getLogger(__name__)

    def load_template_from_file(self):
        loader = jinja2.FileSystemLoader(self.base_path)
        try:
            base_workload = loader.get_source(jinja2.Environment(
                autoescape=select_autoescape(['html', 'xml'])),
                self.template_file_name)
        except jinja2.TemplateNotFound:
            self.logger.exception("Could not load workload from [%s].", self.template_file_name)
            raise WorkloadSyntaxError("Could not load workload from '{}'".format(self.template_file_name))
        self.assembled_source = self.replace_includes(self.base_path, base_workload[0])

    def load_template_from_string(self, template_source):
        self.assembled_source = self.replace_includes(self.base_path, template_source)

    def replace_includes(self, base_path, workload_fragment):
        match = TemplateSource.collect_parts_re.findall(workload_fragment)
        if match:
            # Construct replacement dict for matched captures
            repl = {}
            for glob_pattern in match:
                full_glob_path = os.path.join(base_path, glob_pattern)
                sub_source = self.read_glob_files(full_glob_path)
                repl[glob_pattern] = self.replace_includes(base_path=io.dirname(full_glob_path), workload_fragment=sub_source)

            def replstring(matchobj):
                # matchobj.groups() is a tuple and first element contains the matched group id
                return repl[matchobj.groups()[0]]

            return TemplateSource.collect_parts_re.sub(replstring, workload_fragment)
        return workload_fragment

    def read_glob_files(self, pattern):
        source = []
        files = self.fileglobber(pattern)
        for fname in files:
            with self.source(fname, mode="rt", encoding="utf-8") as fp:
                source.append(fp.read())
        return ",\n".join(source)


def default_internal_template_vars(glob_helper=lambda f: [], clock=time.Clock):
    """
    Dict of internal global variables used by our jinja2 renderers
    """

    return {
        "globals": {
            "now": clock.now(),
            "glob": glob_helper
        },
        "filters": {
            "days_ago": time.days_ago
        }
    }


def render_template(template_source, template_vars=None, template_internal_vars=None, loader=None):
    macros = [
        """
        {% macro collect(parts) -%}
            {% set comma = joiner() %}
            {% for part in glob(parts) %}
                {{ comma() }}
                {% include part %}
            {% endfor %}
        {%- endmacro %}
        """,
        """
        {% macro exists_set_param(setting_name, value, default_value=None, comma=True) -%}
            {% if value is defined or default_value is not none %}
                {% if comma %} , {% endif %}
                {% if default_value is not none %}
                  "{{ setting_name }}": {{ value | default(default_value) | tojson }}
                {% else %}
                  "{{ setting_name }}": {{ value | tojson }}
                {% endif %}
              {% endif %}
        {%- endmacro %}
        """
    ]

    # place helpers dict loader first to prevent users from overriding our macros.
    env = jinja2.Environment(
        loader=jinja2.ChoiceLoader([
            jinja2.DictLoader({"benchmark.helpers": "".join(macros)}),
            jinja2.BaseLoader(),
            loader
        ]),
        autoescape=select_autoescape(['html', 'xml'])
    )

    if template_vars:
        for k, v in template_vars.items():
            env.globals[k] = v
    # ensure that user variables never override our internal variables
    if template_internal_vars:
        for macro_type in template_internal_vars:
            for env_global_key, env_global_value in template_internal_vars[macro_type].items():
                getattr(env, macro_type)[env_global_key] = env_global_value

    template = env.from_string(template_source)
    return template.render()


def register_all_params_in_workload(assembled_source, complete_workload_params=None):
    j2env = jinja2.Environment(autoescape=select_autoescape(['html', 'xml']))

    # we don't need the following j2 filters/macros but we define them anyway to prevent parsing failures
    internal_template_vars = default_internal_template_vars()
    for macro_type in internal_template_vars:
        for env_global_key, env_global_value in internal_template_vars[macro_type].items():
            getattr(j2env, macro_type)[env_global_key] = env_global_value

    ast = j2env.parse(assembled_source)
    j2_variables = meta.find_undeclared_variables(ast)
    if complete_workload_params:
        complete_workload_params.populate_workload_defined_params(j2_variables)


def render_template_from_file(template_file_name, template_vars, complete_workload_params=None):
    def relative_glob(start, f):
        result = glob.glob(os.path.join(start, f))
        if result:
            return [os.path.relpath(p, start) for p in result]
        else:
            return []

    base_path = io.dirname(template_file_name)
    template_source = TemplateSource(base_path, io.basename(template_file_name))
    template_source.load_template_from_file()
    register_all_params_in_workload(template_source.assembled_source, complete_workload_params)

    return render_template(loader=jinja2.FileSystemLoader(base_path),
                           template_source=template_source.assembled_source,
                           template_vars=template_vars,
                           template_internal_vars=default_internal_template_vars(glob_helper=lambda f: relative_glob(base_path, f)))


class TaskFilterWorkloadProcessor(WorkloadProcessor):
    def __init__(self, cfg):
        self.logger = logging.getLogger(__name__)
        include_tasks = cfg.opts("workload", "include.tasks", mandatory=False)
        exclude_tasks = cfg.opts("workload", "exclude.tasks", mandatory=False)

        if include_tasks:
            filtered_tasks = include_tasks
            self.exclude = False
        else:
            filtered_tasks = exclude_tasks
            self.exclude = True
        self.filters = self._filters_from_filtered_tasks(filtered_tasks)

    def _filters_from_filtered_tasks(self, filtered_tasks):
        filters = []
        if filtered_tasks:
            for t in filtered_tasks:
                spec = t.split(":")
                if len(spec) == 1:
                    filters.append(workload.TaskNameFilter(spec[0]))
                elif len(spec) == 2:
                    if spec[0] == "type":
                        filters.append(workload.TaskOpTypeFilter(spec[1]))
                    elif spec[0] == "tag":
                        filters.append(workload.TaskTagFilter(spec[1]))
                    else:
                        raise exceptions.SystemSetupError(f"Invalid format for filtered tasks: [{t}]. "
                                                          f"Expected [type] but got [{spec[0]}].")
                else:
                    raise exceptions.SystemSetupError(f"Invalid format for filtered tasks: [{t}]")
        return filters

    def _filter_out_match(self, task):
        for f in self.filters:
            if task.matches(f):
                if hasattr(task, "tasks") and self.exclude:
                    return False
                return self.exclude
        return not self.exclude

    def on_after_load_workload(self, workload):
        if not self.filters:
            return workload

        for test_procedure in workload.test_procedures:
            # don't modify the schedule while iterating over it
            tasks_to_remove = []
            for task in test_procedure.schedule:
                if self._filter_out_match(task):
                    tasks_to_remove.append(task)
                else:
                    leafs_to_remove = []
                    for leaf_task in task:
                        if self._filter_out_match(leaf_task):
                            leafs_to_remove.append(leaf_task)
                    for leaf_task in leafs_to_remove:
                        self.logger.info("Removing sub-task [%s] from test_procedure [%s] due to task filter.",
                                         leaf_task, test_procedure)
                        task.remove_task(leaf_task)
            for task in tasks_to_remove:
                self.logger.info("Removing task [%s] from test_procedure [%s] due to task filter.", task, test_procedure)
                test_procedure.remove_task(task)

        return workload


class TestModeWorkloadProcessor(WorkloadProcessor):
    def __init__(self, cfg):
        self.test_mode_enabled = cfg.opts("workload", "test.mode.enabled", mandatory=False, default_value=False)
        self.logger = logging.getLogger(__name__)

    def on_after_load_workload(self, workload):
        if not self.test_mode_enabled:
            return workload
        self.logger.info("Preparing workload [%s] for test mode.", str(workload))
        for corpus in workload.corpora:
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug("Reducing corpus size to 1000 documents for [%s]", corpus.name)
            for document_set in corpus.documents:
                # TODO #341: Should we allow this for snapshots too?
                if document_set.is_bulk:
                    document_set.number_of_documents = 1000

                    if document_set.has_compressed_corpus():
                        path, ext = io.splitext(document_set.document_archive)
                        path_2, ext_2 = io.splitext(path)

                        document_set.document_archive = f"{path_2}-1k{ext_2}{ext}"
                        document_set.document_file = f"{path_2}-1k{ext_2}"
                    elif document_set.has_uncompressed_corpus():
                        path, ext = io.splitext(document_set.document_file)
                        document_set.document_file = f"{path}-1k{ext}"
                    else:
                        raise exceptions.BenchmarkAssertionError(f"Document corpus [{corpus.name}] has neither compressed "
                                                             f"nor uncompressed corpus.")

                    # we don't want to check sizes
                    document_set.compressed_size_in_bytes = None
                    document_set.uncompressed_size_in_bytes = None

        for test_procedure in workload.test_procedures:
            for task in test_procedure.schedule:
                # we need iterate over leaf tasks and await iterating over possible intermediate 'parallel' elements
                for leaf_task in task:
                    # iteration-based schedules are divided among all clients and we should provide
                    # at least one iteration for each client.
                    if leaf_task.warmup_iterations is not None and leaf_task.warmup_iterations > leaf_task.clients:
                        count = leaf_task.clients
                        if self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug("Resetting warmup iterations to %d for [%s]", count, str(leaf_task))
                        leaf_task.warmup_iterations = count
                    if leaf_task.iterations is not None and leaf_task.iterations > leaf_task.clients:
                        count = leaf_task.clients
                        if self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug("Resetting measurement iterations to %d for [%s]", count, str(leaf_task))
                        leaf_task.iterations = count
                    if leaf_task.warmup_time_period is not None and leaf_task.warmup_time_period > 0:
                        leaf_task.warmup_time_period = 0
                        if self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug("Resetting warmup time period for [%s] to [%d] seconds.",
                                              str(leaf_task), leaf_task.warmup_time_period)
                    if leaf_task.time_period is not None and leaf_task.time_period > 10:
                        leaf_task.time_period = 10
                        if self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug("Resetting measurement time period for [%s] to [%d] seconds.",
                                              str(leaf_task), leaf_task.time_period)

                    # Keep throttled to expose any errors but increase the target throughput for short execution times.
                    if leaf_task.target_throughput:
                        original_throughput = leaf_task.target_throughput
                        leaf_task.params.pop("target-throughput", None)
                        leaf_task.params.pop("target-interval", None)
                        leaf_task.params["target-throughput"] = f"{sys.maxsize} {original_throughput.unit}"

        return workload


class CompleteWorkloadParams:
    def __init__(self, user_specified_workload_params=None):
        self.workload_defined_params = set()
        self.user_specified_workload_params = user_specified_workload_params if user_specified_workload_params else {}

    def populate_workload_defined_params(self, list_of_workload_params=None):
        self.workload_defined_params.update(set(list_of_workload_params))

    @property
    def sorted_workload_defined_params(self):
        return sorted(self.workload_defined_params)

    def unused_user_defined_workload_params(self):
        set_user_params = set(list(self.user_specified_workload_params.keys()))
        set_user_params.difference_update(self.workload_defined_params)

        return list(set_user_params)


class WorkloadFileReader:
    MINIMUM_SUPPORTED_TRACK_VERSION = 2
    MAXIMUM_SUPPORTED_TRACK_VERSION = 2
    """
    Creates a workload from a workload file.
    """

    def __init__(self, cfg):
        workload_schema_file = os.path.join(cfg.opts("node", "benchmark.root"), "resources", "workload-schema.json")
        with open(workload_schema_file, mode="rt", encoding="utf-8") as f:
            self.workload_schema = json.loads(f.read())
        self.workload_params = cfg.opts("workload", "params", mandatory=False)
        self.complete_workload_params = CompleteWorkloadParams(user_specified_workload_params=self.workload_params)
        self.read_workload = WorkloadSpecificationReader(
            workload_params=self.workload_params,
            complete_workload_params=self.complete_workload_params,
            selected_test_procedure=cfg.opts("workload", "test_procedure.name", mandatory=False)
        )
        self.logger = logging.getLogger(__name__)

    def read(self, workload_name, workload_spec_file, mapping_dir):
        """
        Reads a workload file, verifies it against the JSON schema and if valid, creates a workload.

        :param workload_name: The name of the workload.
        :param workload_spec_file: The complete path to the workload specification file.
        :param mapping_dir: The directory where the mapping files for this workload are stored locally.
        :return: A corresponding workload instance if the workload file is valid.
        """

        self.logger.info("Reading workload specification file [%s].", workload_spec_file)
        # render the workload to a temporary file instead of dumping it into the logs. It is easier to check for error messages
        # involving lines numbers and it also does not bloat Benchmark's log file so much.
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        try:
            rendered = render_template_from_file(
                workload_spec_file, self.workload_params,
                complete_workload_params=self.complete_workload_params)
            with open(tmp.name, "wt", encoding="utf-8") as f:
                f.write(rendered)
            self.logger.info("Final rendered workload for '%s' has been written to '%s'.", workload_spec_file, tmp.name)
            workload_spec = json.loads(rendered)
        except jinja2.exceptions.TemplateNotFound:
            self.logger.exception("Could not load [%s]", workload_spec_file)
            raise exceptions.SystemSetupError("Workload {} does not exist".format(workload_name))
        except json.JSONDecodeError as e:
            self.logger.exception("Could not load [%s].", workload_spec_file)
            msg = "Could not load '{}': {}.".format(workload_spec_file, str(e))
            if e.doc and e.lineno > 0 and e.colno > 0:
                line_idx = e.lineno - 1
                lines = e.doc.split("\n")
                ctx_line_count = 3
                ctx_start = max(0, line_idx - ctx_line_count)
                ctx_end = min(line_idx + ctx_line_count, len(lines))
                erroneous_lines = lines[ctx_start:ctx_end]
                erroneous_lines.insert(line_idx - ctx_start + 1, "-" * (e.colno - 1) + "^ Error is here")
                msg += " Lines containing the error:\n\n{}\n\n".format("\n".join(erroneous_lines))
            msg += "The complete workload has been written to '{}' for diagnosis.".format(tmp.name)
            raise WorkloadSyntaxError(msg)
        except Exception as e:
            self.logger.exception("Could not load [%s].", workload_spec_file)
            msg = "Could not load '{}'. The complete workload has been written to '{}' for diagnosis.".format(workload_spec_file, tmp.name)
            # Convert to string early on to avoid serialization errors with Jinja exceptions.
            raise WorkloadSyntaxError(msg, str(e))
        # check the workload version before even attempting to validate the JSON format to avoid bogus errors.
        raw_version = workload_spec.get("version", WorkloadFileReader.MAXIMUM_SUPPORTED_TRACK_VERSION)
        try:
            workload_version = int(raw_version)
        except ValueError:
            raise exceptions.InvalidSyntax("version identifier for workload %s must be numeric but was [%s]" % (
                workload_name, str(raw_version)))
        if WorkloadFileReader.MINIMUM_SUPPORTED_TRACK_VERSION > workload_version:
            raise exceptions.BenchmarkError("Workload {} is on version {} but needs to be updated at least to version {} to work with the "
                                        "current version of Benchmark.".format(workload_name, workload_version,
                                                                           WorkloadFileReader.MINIMUM_SUPPORTED_TRACK_VERSION))
        if WorkloadFileReader.MAXIMUM_SUPPORTED_TRACK_VERSION < workload_version:
            raise exceptions.BenchmarkError("Workload {} requires a newer version of Benchmark. "
                        "Please upgrade Benchmark (supported workload version: {}, "
                                        "required workload version: {}).".format(
                                            workload_name,
                                            WorkloadFileReader.MAXIMUM_SUPPORTED_TRACK_VERSION,
                                                                              workload_version))
        try:
            jsonschema.validate(workload_spec, self.workload_schema)
        except jsonschema.exceptions.ValidationError as ve:
            raise WorkloadSyntaxError(
                "Workload '{}' is invalid.\n\nError details: {}\nInstance: {}\nPath: {}\nSchema path: {}".format(
                    workload_name, ve.message, json.dumps(
                        ve.instance, indent=4, sort_keys=True),
                        ve.absolute_path, ve.absolute_schema_path))

        current_workload = self.read_workload(workload_name, workload_spec, mapping_dir)

        unused_user_defined_workload_params = self.complete_workload_params.unused_user_defined_workload_params()
        if len(unused_user_defined_workload_params) > 0:
            err_msg = (
                "Some of your workload parameter(s) {} are not used by this workload; perhaps you intend to use {} instead.\n\n"
                "All workload parameters you provided are:\n"
                "{}\n\n"
                "All parameters exposed by this workload:\n"
                "{}".format(
                    ",".join(opts.double_quoted_list_of(sorted(unused_user_defined_workload_params))),
                    ",".join(opts.double_quoted_list_of(sorted(opts.make_list_of_close_matches(
                        unused_user_defined_workload_params,
                        self.complete_workload_params.workload_defined_params
                    )))),
                    "\n".join(opts.bulleted_list_of(sorted(list(self.workload_params.keys())))),
                    "\n".join(opts.bulleted_list_of(self.complete_workload_params.sorted_workload_defined_params))))

            self.logger.critical(err_msg)
            # also dump the message on the console
            console.println(err_msg)
            raise exceptions.WorkloadConfigError(
                "Unused workload parameters {}.".format(sorted(unused_user_defined_workload_params))
            )
        return current_workload


class WorkloadPluginReader:
    """
    Loads workload plugins
    """

    def __init__(self, workload_plugin_path, runner_registry=None, scheduler_registry=None, workload_processor_registry=None):
        self.runner_registry = runner_registry
        self.scheduler_registry = scheduler_registry
        self.workload_processor_registry = workload_processor_registry
        self.loader = modules.ComponentLoader(root_path=workload_plugin_path, component_entry_point="workload")

    def can_load(self):
        return self.loader.can_load()

    def load(self):
        root_module = self.loader.load()
        try:
            # every module needs to have a register() method
            root_module.register(self)
        except BaseException:
            msg = "Could not register workload plugin at [%s]" % self.loader.root_path
            logging.getLogger(__name__).exception(msg)
            raise exceptions.SystemSetupError(msg)

    def register_param_source(self, name, param_source):
        params.register_param_source_for_name(name, param_source)

    def register_runner(self, name, runner, **kwargs):
        if self.runner_registry:
            self.runner_registry(name, runner, **kwargs)

    def register_scheduler(self, name, scheduler):
        if self.scheduler_registry:
            self.scheduler_registry(name, scheduler)

    def register_workload_processor(self, workload_processor):
        if self.workload_processor_registry:
            self.workload_processor_registry(workload_processor)

    @property
    def meta_data(self):
        return {
            "benchmark_version": version.release_version(),
            "async_runner": True
        }


class WorkloadSpecificationReader:
    """
    Creates a workload instances based on its parsed JSON description.
    """

    def __init__(self, workload_params=None, complete_workload_params=None, selected_test_procedure=None, source=io.FileSource):
        self.name = None
        self.workload_params = workload_params if workload_params else {}
        self.complete_workload_params = complete_workload_params
        self.selected_test_procedure = selected_test_procedure
        self.source = source
        self.logger = logging.getLogger(__name__)

    def __call__(self, workload_name, workload_specification, mapping_dir):
        self.name = workload_name
        description = self._r(workload_specification, "description", mandatory=False, default_value="")

        meta_data = self._r(workload_specification, "meta", mandatory=False)
        indices = [self._create_index(idx, mapping_dir)
                   for idx in self._r(workload_specification, "indices", mandatory=False, default_value=[])]
        data_streams = [self._create_data_stream(idx)
                        for idx in self._r(workload_specification, "data-streams", mandatory=False, default_value=[])]
        if len(indices) > 0 and len(data_streams) > 0:
            # we guard against this early and support either or
            raise WorkloadSyntaxError("indices and data-streams cannot both be specified")
        templates = [self._create_index_template(tpl, mapping_dir)
                     for tpl in self._r(workload_specification, "templates", mandatory=False, default_value=[])]
        composable_templates = [self._create_index_template(tpl, mapping_dir)
                     for tpl in self._r(workload_specification, "composable-templates", mandatory=False, default_value=[])]
        component_templates = [self._create_component_template(tpl, mapping_dir)
                     for tpl in self._r(workload_specification, "component-templates", mandatory=False, default_value=[])]
        corpora = self._create_corpora(self._r(workload_specification, "corpora", mandatory=False, default_value=[]),
                                       indices, data_streams)
        test_procedures = self._create_test_procedures(workload_specification)
        # at this point, *all* workload params must have been referenced in the templates
        return workload.Workload(name=self.name, meta_data=meta_data,
        description=description, test_procedures=test_procedures,
        indices=indices,
                           data_streams=data_streams, templates=templates, composable_templates=composable_templates,
                           component_templates=component_templates, corpora=corpora)

    def _error(self, msg):
        raise WorkloadSyntaxError("Workload '%s' is invalid. %s" % (self.name, msg))

    def _r(self, root, path, error_ctx=None, mandatory=True, default_value=None):
        if isinstance(path, str):
            path = [path]

        structure = root
        try:
            for k in path:
                structure = structure[k]
            return structure
        except KeyError:
            if mandatory:
                if error_ctx:
                    self._error("Mandatory element '%s' is missing in '%s'." % (".".join(path), error_ctx))
                else:
                    self._error("Mandatory element '%s' is missing." % ".".join(path))
            else:
                return default_value

    def _create_index(self, index_spec, mapping_dir):
        index_name = self._r(index_spec, "name")
        body_file = self._r(index_spec, "body", mandatory=False)
        if body_file:
            idx_body_tmpl_src = TemplateSource(mapping_dir, body_file, self.source)
            with self.source(os.path.join(mapping_dir, body_file), "rt") as f:
                idx_body_tmpl_src.load_template_from_string(f.read())
                body = self._load_template(
                    idx_body_tmpl_src.assembled_source,
                    "definition for index {} in {}".format(index_name, body_file))
        else:
            body = None

        return workload.Index(name=index_name, body=body, types=self._r(index_spec, "types", mandatory=False, default_value=[]))

    def _create_data_stream(self, data_stream_spec):
        return workload.DataStream(name=self._r(data_stream_spec, "name"))

    def _create_component_template(self, tpl_spec, mapping_dir):
        name = self._r(tpl_spec, "name")
        template_file = self._r(tpl_spec, "template")
        template_file = os.path.join(mapping_dir, template_file)
        idx_tmpl_src = TemplateSource(mapping_dir, template_file, self.source)
        with self.source(template_file, "rt") as f:
            idx_tmpl_src.load_template_from_string(f.read())
            template_content = self._load_template(
                idx_tmpl_src.assembled_source,
                f"definition for component template {name} in {template_file}")
        return workload.ComponentTemplate(name, template_content)

    def _create_index_template(self, tpl_spec, mapping_dir):
        name = self._r(tpl_spec, "name")
        template_file = self._r(tpl_spec, "template")
        index_pattern = self._r(tpl_spec, "index-pattern")
        delete_matching_indices = self._r(tpl_spec, "delete-matching-indices", mandatory=False, default_value=True)
        template_file = os.path.join(mapping_dir, template_file)
        idx_tmpl_src = TemplateSource(mapping_dir, template_file, self.source)
        with self.source(template_file, "rt") as f:
            idx_tmpl_src.load_template_from_string(f.read())
            template_content = self._load_template(
                idx_tmpl_src.assembled_source,
                "definition for index template {} in {}".format(name, template_file))
        return workload.IndexTemplate(name, index_pattern, template_content, delete_matching_indices)

    def _load_template(self, contents, description):
        self.logger.info("Loading template [%s].", description)
        register_all_params_in_workload(contents, self.complete_workload_params)
        try:
            rendered = render_template(template_source=contents,
                                       template_vars=self.workload_params)
            return json.loads(rendered)
        except Exception as e:
            self.logger.exception("Could not load file template for %s.", description)
            raise WorkloadSyntaxError("Could not load file template for '%s'" % description, str(e))

    def _create_corpora(self, corpora_specs, indices, data_streams):
        if len(indices) > 0 and len(data_streams) > 0:
            raise WorkloadSyntaxError("indices and data-streams cannot both be specified")
        document_corpora = []
        known_corpora_names = set()
        for corpus_spec in corpora_specs:
            name = self._r(corpus_spec, "name")

            if name in known_corpora_names:
                self._error("Duplicate document corpus name [%s]." % name)
            known_corpora_names.add(name)

            meta_data = self._r(corpus_spec, "meta", error_ctx=name, mandatory=False)
            corpus = workload.DocumentCorpus(name=name, meta_data=meta_data)
            # defaults on corpus level
            default_base_url = self._r(corpus_spec, "base-url", mandatory=False, default_value=None)
            default_source_format = self._r(corpus_spec, "source-format", mandatory=False,
                                            default_value=workload.Documents.SOURCE_FORMAT_BULK)
            default_action_and_meta_data = self._r(corpus_spec, "includes-action-and-meta-data", mandatory=False,
                                                   default_value=False)
            corpus_target_idx = None
            corpus_target_ds = None
            corpus_target_type = None

            if len(indices) == 1:
                corpus_target_idx = self._r(corpus_spec, "target-index", mandatory=False, default_value=indices[0].name)
            elif len(indices) > 0:
                corpus_target_idx = self._r(corpus_spec, "target-index", mandatory=False)

            if len(data_streams) == 1:
                corpus_target_ds = self._r(corpus_spec, "target-data-stream", mandatory=False,
                                           default_value=data_streams[0].name)
            elif len(data_streams) > 0:
                corpus_target_ds = self._r(corpus_spec, "target-data-stream", mandatory=False)

            if len(indices) == 1 and len(indices[0].types) == 1:
                corpus_target_type = self._r(corpus_spec, "target-type", mandatory=False,
                                             default_value=indices[0].types[0])
            elif len(indices) > 0:
                corpus_target_type = self._r(corpus_spec, "target-type", mandatory=False)

            for doc_spec in self._r(corpus_spec, "documents"):
                base_url = self._r(doc_spec, "base-url", mandatory=False, default_value=default_base_url)
                source_format = self._r(doc_spec, "source-format", mandatory=False, default_value=default_source_format)

                if source_format == workload.Documents.SOURCE_FORMAT_BULK:
                    docs = self._r(doc_spec, "source-file")
                    if io.is_archive(docs):
                        document_archive = docs
                        document_file = io.splitext(docs)[0]
                    else:
                        document_archive = None
                        document_file = docs
                    num_docs = self._r(doc_spec, "document-count")
                    compressed_bytes = self._r(doc_spec, "compressed-bytes", mandatory=False)
                    uncompressed_bytes = self._r(doc_spec, "uncompressed-bytes", mandatory=False)
                    doc_meta_data = self._r(doc_spec, "meta", error_ctx=name, mandatory=False)

                    includes_action_and_meta_data = self._r(doc_spec, "includes-action-and-meta-data", mandatory=False,
                                                            default_value=default_action_and_meta_data)
                    if includes_action_and_meta_data:
                        target_idx = None
                        target_type = None
                        target_ds = None
                    else:
                        target_type = self._r(doc_spec, "target-type", mandatory=False,
                                              default_value=corpus_target_type, error_ctx=docs)

                        # require to be specified if we're using data streams and we have no default
                        target_ds = self._r(doc_spec, "target-data-stream",
                                            mandatory=len(data_streams) > 0 and corpus_target_ds is None,
                                            default_value=corpus_target_ds,
                                            error_ctx=docs)
                        if target_ds and len(indices) > 0:
                            # if indices are in use we error
                            raise WorkloadSyntaxError("target-data-stream cannot be used when using indices")
                        elif target_ds and target_type:
                            raise WorkloadSyntaxError("target-type cannot be used when using data-streams")

                        # need an index if we're using indices and no meta-data are present and we don't have a default
                        target_idx = self._r(doc_spec, "target-index",
                                             mandatory=len(indices) > 0 and corpus_target_idx is None,
                                             default_value=corpus_target_idx,
                                             error_ctx=docs)
                        # either target_idx or target_ds
                        if target_idx and len(data_streams) > 0:
                            # if data streams are in use we error
                            raise WorkloadSyntaxError("target-index cannot be used when using data-streams")

                        # we need one or the other
                        if target_idx is None and target_ds is None:
                            raise WorkloadSyntaxError(f"a {'target-index' if len(indices) > 0 else 'target-data-stream'} "
                                                   f"is required for {docs}" )

                    docs = workload.Documents(source_format=source_format,
                                           document_file=document_file,
                                           document_archive=document_archive,
                                           base_url=base_url,
                                           includes_action_and_meta_data=includes_action_and_meta_data,
                                           number_of_documents=num_docs,
                                           compressed_size_in_bytes=compressed_bytes,
                                           uncompressed_size_in_bytes=uncompressed_bytes,
                                           target_index=target_idx, target_type=target_type,
                                           target_data_stream=target_ds, meta_data=doc_meta_data)
                    corpus.documents.append(docs)
                else:
                    self._error("Unknown source-format [%s] in document corpus [%s]." % (source_format, name))
            document_corpora.append(corpus)
        return document_corpora

    def _create_test_procedures(self, workload_spec):
        ops = self.parse_operations(self._r(workload_spec, "operations", mandatory=False, default_value=[]))
        workload_params = self._r(workload_spec, "parameters", mandatory=False, default_value={})
        test_procedures = []
        known_test_procedure_names = set()
        default_test_procedure = None
        test_procedure_specs, auto_generated = self._get_test_procedure_specs(workload_spec)
        number_of_test_procedures = len(test_procedure_specs)
        for test_procedure_spec in test_procedure_specs:
            name = self._r(test_procedure_spec, "name", error_ctx="test_procedures")
            description = self._r(test_procedure_spec, "description", error_ctx=name, mandatory=False)
            user_info = self._r(test_procedure_spec, "user-info", error_ctx=name, mandatory=False)
            test_procedure_params = self._r(test_procedure_spec, "parameters", error_ctx=name, mandatory=False, default_value={})
            meta_data = self._r(test_procedure_spec, "meta", error_ctx=name, mandatory=False)
            # if we only have one test_procedure it is treated as default test_procedure, no matter what the user has specified
            default = number_of_test_procedures == 1 or self._r(test_procedure_spec, "default", error_ctx=name, mandatory=False)
            selected = number_of_test_procedures == 1 or self.selected_test_procedure == name
            if default and default_test_procedure is not None:
                self._error("Both '%s' and '%s' are defined as default test_procedures. Please define only one of them as default."
                            % (default_test_procedure.name, name))
            if name in known_test_procedure_names:
                self._error("Duplicate test_procedure with name '%s'." % name)
            known_test_procedure_names.add(name)

            schedule = []

            for op in self._r(test_procedure_spec, "schedule", error_ctx=name):
                if "parallel" in op:
                    task = self.parse_parallel(op["parallel"], ops, name)
                else:
                    task = self.parse_task(op, ops, name)
                schedule.append(task)

            # verify we don't have any duplicate task names (which can be confusing / misleading in results_publishing).
            known_task_names = set()
            for task in schedule:
                for sub_task in task:
                    if sub_task.name in known_task_names:
                        self._error("TestProcedure '%s' contains multiple tasks with the name '%s'. Please use the task's name property to "
                                    "assign a unique name for each task." % (name, sub_task.name))
                    else:
                        known_task_names.add(sub_task.name)

            # merge params
            final_test_procedure_params = dict(collections.merge_dicts(workload_params, test_procedure_params))

            test_procedure = workload.TestProcedure(name=name,
                                        parameters=final_test_procedure_params,
                                        meta_data=meta_data,
                                        description=description,
                                        user_info=user_info,
                                        default=default,
                                        selected=selected,
                                        auto_generated=auto_generated,
                                        schedule=schedule)
            if default:
                default_test_procedure = test_procedure

            test_procedures.append(test_procedure)

        if test_procedures and default_test_procedure is None:
            self._error(
                "No default test_procedure specified. Please edit the workload and add \"default\": true to one of the test_procedures %s."
                        % ", ".join([c.name for c in test_procedures]))
        return test_procedures

    def _get_test_procedure_specs(self, workload_spec):
        schedule = self._r(workload_spec, "schedule", mandatory=False)
        test_procedure = self._r(workload_spec, "test_procedure", mandatory=False)
        test_procedures = self._r(workload_spec, "test_procedures", mandatory=False)

        count_defined = len(list(filter(lambda e: e is not None, [schedule, test_procedure, test_procedures])))

        if count_defined == 0:
            self._error("You must define 'test_procedure', 'test_procedures' or 'schedule' but none is specified.")
        elif count_defined > 1:
            self._error("Multiple out of 'test_procedure', 'test_procedures' or 'schedule' are defined but only one of them is allowed.")
        elif test_procedure is not None:
            return [test_procedure], False
        elif test_procedures is not None:
            return test_procedures, False
        elif schedule is not None:
            return [{
                "name": "default",
                "schedule": schedule
            }], True
        else:
            raise AssertionError(
                "Unexpected: schedule=[{}], test_procedure=[{}], test_procedures=[{}]".format(
                    schedule, test_procedure, test_procedures))

    def parse_parallel(self, ops_spec, ops, test_procedure_name):
        # use same default values as #parseTask() in case the 'parallel' element did not specify anything
        default_warmup_iterations = self._r(ops_spec, "warmup-iterations", error_ctx="parallel", mandatory=False)
        default_iterations = self._r(ops_spec, "iterations", error_ctx="parallel", mandatory=False)
        default_warmup_time_period = self._r(ops_spec, "warmup-time-period", error_ctx="parallel", mandatory=False)
        default_time_period = self._r(ops_spec, "time-period", error_ctx="parallel", mandatory=False)
        clients = self._r(ops_spec, "clients", error_ctx="parallel", mandatory=False)
        completed_by = self._r(ops_spec, "completed-by", error_ctx="parallel", mandatory=False)

        # now descent to each operation
        tasks = []
        for task in self._r(ops_spec, "tasks", error_ctx="parallel"):
            tasks.append(self.parse_task(task, ops, test_procedure_name, default_warmup_iterations, default_iterations,
                                         default_warmup_time_period, default_time_period, completed_by))
        if completed_by:
            completion_task = None
            for task in tasks:
                if task.completes_parent and not completion_task:
                    completion_task = task
                elif task.completes_parent:
                    self._error(
                        "'parallel' element for test_procedure '%s' contains multiple tasks with the name '%s' which are marked with "
                                "'completed-by' but only task is allowed to match." % (test_procedure_name, completed_by))
            if not completion_task:
                self._error("'parallel' element for test_procedure '%s' is marked with 'completed-by' with task name '%s' but no task with "
                            "this name exists." % (test_procedure_name, completed_by))
        return workload.Parallel(tasks, clients)

    def parse_task(self, task_spec, ops, test_procedure_name, default_warmup_iterations=None, default_iterations=None,
                   default_warmup_time_period=None, default_time_period=None, completed_by_name=None):

        op_spec = task_spec["operation"]
        if isinstance(op_spec, str) and op_spec in ops:
            op = ops[op_spec]
        else:
            # may as well an inline operation
            op = self.parse_operation(op_spec, error_ctx="inline operation in test_procedure %s" % test_procedure_name)

        schedule = self._r(task_spec, "schedule", error_ctx=op.name, mandatory=False)
        task_name = self._r(task_spec, "name", error_ctx=op.name, mandatory=False, default_value=op.name)
        task = workload.Task(name=task_name,
                          operation=op,
                          tags=self._r(task_spec, "tags", error_ctx=op.name, mandatory=False),
                          meta_data=self._r(task_spec, "meta", error_ctx=op.name, mandatory=False),
                          warmup_iterations=self._r(task_spec, "warmup-iterations", error_ctx=op.name, mandatory=False,
                                                    default_value=default_warmup_iterations),
                          iterations=self._r(task_spec, "iterations", error_ctx=op.name, mandatory=False, default_value=default_iterations),
                          warmup_time_period=self._r(task_spec, "warmup-time-period", error_ctx=op.name,
                                                     mandatory=False,
                                                     default_value=default_warmup_time_period),
                          time_period=self._r(task_spec, "time-period", error_ctx=op.name, mandatory=False,
                                              default_value=default_time_period),
                          clients=self._r(task_spec, "clients", error_ctx=op.name, mandatory=False, default_value=1),
                          completes_parent=(task_name == completed_by_name),
                          schedule=schedule,
                          # this is to provide scheduler-specific parameters for custom schedulers.
                          params=task_spec)
        if task.warmup_iterations is not None and task.time_period is not None:
            self._error(
                "Operation '%s' in test_procedure '%s' defines '%d' warmup iterations and a time period of '%d' seconds. Please do not "
                        "mix time periods and iterations." % (op.name, test_procedure_name, task.warmup_iterations, task.time_period))
        elif task.warmup_time_period is not None and task.iterations is not None:
            self._error(
                "Operation '%s' in test_procedure '%s' defines a warmup time period of '%d' seconds and '%d' iterations. Please do not "
                        "mix time periods and iterations." % (op.name, test_procedure_name, task.warmup_time_period, task.iterations))

        return task

    def parse_operations(self, ops_specs):
        # key = name, value = operation
        ops = {}
        for op_spec in ops_specs:
            op = self.parse_operation(op_spec)
            if op.name in ops:
                self._error("Duplicate operation with name '%s'." % op.name)
            else:
                ops[op.name] = op
        return ops

    def parse_operation(self, op_spec, error_ctx="operations"):
        # just a name, let's assume it is a simple operation like force-merge and create a full operation
        if isinstance(op_spec, str):
            op_name = op_spec
            meta_data = None
            op_type_name = op_spec
            param_source = None
            # Cannot have parameters here
            params = {}
        else:
            meta_data = self._r(op_spec, "meta", error_ctx=error_ctx, mandatory=False)
            # Benchmark's core operations will still use enums then but we'll allow users to define arbitrary operations
            op_type_name = self._r(op_spec, "operation-type", error_ctx=error_ctx)
            # fallback to use the operation type as the operation name
            op_name = self._r(op_spec, "name", error_ctx=error_ctx, mandatory=False, default_value=op_type_name)
            param_source = self._r(op_spec, "param-source", error_ctx=error_ctx, mandatory=False)
            # just pass-through all parameters by default
            params = op_spec

        try:
            op = workload.OperationType.from_hyphenated_string(op_type_name)
            if "include-in-results_publishing" not in params:
                params["include-in-results_publishing"] = not op.admin_op
            self.logger.debug("Using built-in operation type [%s] for operation [%s].", op_type_name, op_name)
        except KeyError:
            self.logger.info("Using user-provided operation type [%s] for operation [%s].", op_type_name, op_name)

        try:
            return workload.Operation(name=op_name, meta_data=meta_data,
            operation_type=op_type_name, params=params,
            param_source=param_source)
        except exceptions.InvalidSyntax as e:
            raise WorkloadSyntaxError("Invalid operation [%s]: %s" % (op_name, str(e)))
