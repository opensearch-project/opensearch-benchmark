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

import collections
import logging
import os
import sys

import tabulate
import thespian.actors

from osbenchmark import actor, config, doc_link, \
    worker_coordinator, exceptions, builder, metrics, \
        results_publisher, workload, version, PROGRAM_NAME
from osbenchmark.utils import console, opts, versions


pipelines = collections.OrderedDict()


class Pipeline:
    """
    Describes a whole execution pipeline. A pipeline can consist of one or more steps. Each pipeline should contain roughly of the following
    steps:

    * Prepare the benchmark candidate: It can build OpenSearch from sources, download a ZIP from somewhere etc.
    * Launch the benchmark candidate: This can be done directly, with tools like Ansible or it can assume the candidate is already launched
    * Run the benchmark
    * Publish results
    """

    def __init__(self, name, description, target, stable=True):
        """
        Creates a new pipeline.

        :param name: A short name of the pipeline. This name will be used to reference it from the command line.
        :param description: A human-readable description what the pipeline does.
        :param target: A function that implements this pipeline
        :param stable True iff the pipeline is considered production quality.
        """
        self.name = name
        self.description = description
        self.target = target
        self.stable = stable
        pipelines[name] = self

    def __call__(self, cfg):
        self.target(cfg)


class Setup:
    def __init__(self, cfg, sources=False, distribution=False, external=False, docker=False):
        self.cfg = cfg
        self.sources = sources
        self.distribution = distribution
        self.external = external
        self.docker = docker


class Success:
    pass


class BenchmarkActor(actor.BenchmarkActor):
    def __init__(self):
        super().__init__()
        self.cfg = None
        self.start_sender = None
        self.builder = None
        self.main_worker_coordinator = None
        self.coordinator = None

    def receiveMsg_PoisonMessage(self, msg, sender):
        self.logger.info("BenchmarkActor got notified of poison message [%s] (forwarding).", (str(msg)))
        if self.coordinator:
            self.coordinator.error = True
        self.send(self.start_sender, msg)

    def receiveUnrecognizedMessage(self, msg, sender):
        self.logger.info("BenchmarkActor received unknown message [%s] (ignoring).", (str(msg)))

    @actor.no_retry("test execution orchestrator")  # pylint: disable=no-value-for-parameter
    def receiveMsg_Setup(self, msg, sender):
        self.start_sender = sender
        self.cfg = msg.cfg
        self.coordinator = BenchmarkCoordinator(msg.cfg)
        self.coordinator.setup(sources=msg.sources)
        self.logger.info("Asking builder to start the engine.")
        self.builder = self.createActor(builder.BuilderActor, targetActorRequirements={"coordinator": True})
        self.send(self.builder, builder.StartEngine(self.cfg,
                                                      self.coordinator.metrics_store.open_context,
                                                      msg.sources,
                                                      msg.distribution,
                                                      msg.external,
                                                      msg.docker))

    @actor.no_retry("test execution orchestrator")  # pylint: disable=no-value-for-parameter
    def receiveMsg_EngineStarted(self, msg, sender):
        self.logger.info("Builder has started engine successfully.")
        self.coordinator.test_execution.provision_config_revision = msg.provision_config_revision
        self.main_worker_coordinator = self.createActor(
            worker_coordinator.WorkerCoordinatorActor,
            targetActorRequirements={"coordinator": True}
            )
        self.logger.info("Telling worker_coordinator to prepare for benchmarking.")
        self.send(self.main_worker_coordinator, worker_coordinator.PrepareBenchmark(self.cfg, self.coordinator.current_workload))

    @actor.no_retry("test execution orchestrator")  # pylint: disable=no-value-for-parameter
    def receiveMsg_PreparationComplete(self, msg, sender):
        self.coordinator.on_preparation_complete(msg.distribution_flavor, msg.distribution_version, msg.revision)
        self.logger.info("Telling worker_coordinator to start benchmark.")
        self.send(self.main_worker_coordinator, worker_coordinator.StartBenchmark())

    @actor.no_retry("test execution orchestrator")  # pylint: disable=no-value-for-parameter
    def receiveMsg_TaskFinished(self, msg, sender):
        self.coordinator.on_task_finished(msg.metrics)
        # We choose *NOT* to reset our own metrics store's timer as this one is only used to collect complete metrics records from
        # other stores (used by worker_coordinator and builder). Hence there is no need to reset the timer in our own metrics store.
        self.send(self.builder, builder.ResetRelativeTime(msg.next_task_scheduled_in))

    @actor.no_retry("test execution orchestrator")  # pylint: disable=no-value-for-parameter
    def receiveMsg_BenchmarkCancelled(self, msg, sender):
        self.coordinator.cancelled = True
        # even notify the start sender if it is the originator. The reason is that we call #ask() which waits for a reply.
        # We also need to ask in order to avoid test_executions between this notification and the following ActorExitRequest.
        self.send(self.start_sender, msg)

    @actor.no_retry("test execution orchestrator")  # pylint: disable=no-value-for-parameter
    def receiveMsg_BenchmarkFailure(self, msg, sender):
        self.logger.info("Received a benchmark failure from [%s] and will forward it now.", sender)
        self.coordinator.error = True
        self.send(self.start_sender, msg)

    @actor.no_retry("test execution orchestrator")  # pylint: disable=no-value-for-parameter
    def receiveMsg_BenchmarkComplete(self, msg, sender):
        self.coordinator.on_benchmark_complete(msg.metrics)
        self.send(self.main_worker_coordinator, thespian.actors.ActorExitRequest())
        self.main_worker_coordinator = None
        self.logger.info("Asking builder to stop the engine.")
        self.send(self.builder, builder.StopEngine())

    @actor.no_retry("test execution orchestrator")  # pylint: disable=no-value-for-parameter
    def receiveMsg_EngineStopped(self, msg, sender):
        self.logger.info("Builder has stopped engine successfully.")
        self.send(self.start_sender, Success())


class BenchmarkCoordinator:
    def __init__(self, cfg):
        self.logger = logging.getLogger(__name__)
        self.cfg = cfg
        self.test_execution = None
        self.metrics_store = None
        self.test_execution_store = None
        self.cancelled = False
        self.error = False
        self.workload_revision = None
        self.current_workload = None
        self.current_test_procedure = None

    def setup(self, sources=False):
        # to load the workload we need to know the correct cluster distribution version. Usually, this value should be set
        # but there are rare cases (external pipeline and user did not specify the distribution version) where we need
        # to derive it ourselves. For source builds we always assume "master"
        oss_distribution_version = "2.11.0"
        if not sources and not self.cfg.exists("builder", "distribution.version"):
            distribution_version = builder.cluster_distribution_version(self.cfg)
            if distribution_version == 'oss':
                self.logger.info("Automatically derived serverless collection, setting distribution version to 2.11.0")
                distribution_version = oss_distribution_version
                if not self.cfg.exists("worker_coordinator", "serverless.mode"):
                    self.cfg.add(config.Scope.benchmark, "worker_coordinator", "serverless.mode", True)

                if not self.cfg.exists("worker_coordinator", "serverless.operator"):
                    self.cfg.add(config.Scope.benchmark, "worker_coordinator", "serverless.operator", True)
            else:
                self.logger.info("Automatically derived distribution version [%s]", distribution_version)
            self.cfg.add(config.Scope.benchmark, "builder", "distribution.version", distribution_version)
            min_os_version = versions.Version.from_string(version.minimum_os_version())
            specified_version = versions.Version.from_string(distribution_version)
            if specified_version < min_os_version:
                raise exceptions.SystemSetupError(f"Cluster version must be at least [{min_os_version}] but was [{distribution_version}]")

        self.current_workload = workload.load_workload(self.cfg)
        self.workload_revision = self.cfg.opts("workload", "repository.revision", mandatory=False)
        test_procedure_name = self.cfg.opts("workload", "test_procedure.name")
        self.current_test_procedure = self.current_workload.find_test_procedure_or_default(test_procedure_name)
        if self.current_test_procedure is None:
            raise exceptions.SystemSetupError(
                "Workload [{}] does not provide test_procedure [{}]. List the available workloads with {} list workloads.".format(
                    self.current_workload.name, test_procedure_name, PROGRAM_NAME))
        if self.current_test_procedure.user_info:
            console.info(self.current_test_procedure.user_info)

        for info in self.current_test_procedure.serverless_info:
            console.info(info)

        self.test_execution = metrics.create_test_execution(
            self.cfg, self.current_workload,
            self.current_test_procedure,
            self.workload_revision)

        self.metrics_store = metrics.metrics_store(
            self.cfg,
            workload=self.test_execution.workload_name,
            test_procedure=self.test_execution.test_procedure_name,
            read_only=False
        )
        self.test_execution_store = metrics.test_execution_store(self.cfg)

    def on_preparation_complete(self, distribution_flavor, distribution_version, revision):
        self.test_execution.distribution_flavor = distribution_flavor
        self.test_execution.distribution_version = distribution_version
        self.test_execution.revision = revision
        # store test_execution initially (without any results) so other components can retrieve full metadata
        self.test_execution_store.store_test_execution(self.test_execution)
        if self.test_execution.test_procedure.auto_generated:
            console.info("Executing test with workload [{}] and provision_config_instance {} with version [{}].\n"
                         .format(self.test_execution.workload_name,
                         self.test_execution.provision_config_instance,
                         self.test_execution.distribution_version))
        else:
            console.info("Executing test with workload [{}], test_procedure [{}] and provision_config_instance {} with version [{}].\n"
                         .format(
                             self.test_execution.workload_name,
                             self.test_execution.test_procedure_name,
                             self.test_execution.provision_config_instance,
                             self.test_execution.distribution_version
                             ))

    def on_task_finished(self, new_metrics):
        self.logger.info("Task has finished.")
        self.logger.info("Bulk adding request metrics to metrics store.")
        self.metrics_store.bulk_add(new_metrics)

    def on_benchmark_complete(self, new_metrics):
        self.logger.info("OSB is complete.")
        self.logger.info("Bulk adding request metrics to metrics store.")
        self.metrics_store.bulk_add(new_metrics)
        self.metrics_store.flush()
        if not self.cancelled and not self.error:
            final_results = metrics.calculate_results(self.metrics_store, self.test_execution)
            self.test_execution.add_results(final_results)
            self.test_execution_store.store_test_execution(self.test_execution)
            metrics.results_store(self.cfg).store_results(self.test_execution)
            results_publisher.summarize(final_results, self.cfg)
        else:
            self.logger.info("Suppressing output of summary results. Cancelled = [%r], Error = [%r].", self.cancelled, self.error)
        self.metrics_store.close()


def execute_test(cfg, sources=False, distribution=False, external=False, docker=False):
    logger = logging.getLogger(__name__)
    # at this point an actor system has to run and we should only join
    actor_system = actor.bootstrap_actor_system(try_join=True)
    benchmark_actor = actor_system.createActor(BenchmarkActor, targetActorRequirements={"coordinator": True})
    try:
        result = actor_system.ask(benchmark_actor, Setup(cfg, sources, distribution, external, docker))
        if isinstance(result, Success):
            logger.info("OSB has finished successfully.")
        # may happen if one of the load generators has detected that the user has cancelled the benchmark.
        elif isinstance(result, actor.BenchmarkCancelled):
            logger.info("User has cancelled the benchmark (detected by actor).")
        elif isinstance(result, actor.BenchmarkFailure):
            logger.error("A benchmark failure has occurred")
            raise exceptions.BenchmarkError(result.message, result.cause)
        else:
            raise exceptions.BenchmarkError("Got an unexpected result during benchmarking: [%s]." % str(result))
    except KeyboardInterrupt:
        logger.info("User has cancelled the benchmark (detected by test execution orchestrator).")
        # notify the coordinator so it can properly handle this state. Do it blocking so we don't have a test execution between this message
        # and the actor exit request.
        actor_system.ask(benchmark_actor, actor.BenchmarkCancelled())
    finally:
        logger.info("Telling benchmark actor to exit.")
        actor_system.tell(benchmark_actor, thespian.actors.ActorExitRequest())


def set_default_hosts(cfg, host="127.0.0.1", port=9200):
    logger = logging.getLogger(__name__)
    configured_hosts = cfg.opts("client", "hosts")
    if len(configured_hosts.default) != 0:
        logger.info("Using configured hosts %s", configured_hosts.default)
    else:
        logger.info("Setting default host to [%s:%d]", host, port)
        default_host_object = opts.TargetHosts("{}:{}".format(host,port))
        cfg.add(config.Scope.benchmark, "client", "hosts", default_host_object)


# Poor man's curry
def from_sources(cfg):
    port = cfg.opts("provisioning", "node.http.port")
    set_default_hosts(cfg, port=port)
    return execute_test(cfg, sources=True)


def from_distribution(cfg):
    port = cfg.opts("provisioning", "node.http.port")
    set_default_hosts(cfg, port=port)
    return execute_test(cfg, distribution=True)


def benchmark_only(cfg):
    set_default_hosts(cfg)
    # We'll use a special provision_config_instance name for external benchmarks.
    cfg.add(config.Scope.benchmark, "builder", "provision_config_instance.names", ["external"])
    return execute_test(cfg, external=True)


def docker(cfg):
    set_default_hosts(cfg)
    return execute_test(cfg, docker=True)


Pipeline("from-sources",
         "Builds and provisions OpenSearch, runs a benchmark and publishes results.", from_sources)

Pipeline("from-distribution",
         "Downloads an OpenSearch distribution, provisions it, runs a benchmark and publishes results.", from_distribution)

Pipeline("benchmark-only",
         "Assumes an already running OpenSearch instance, runs a benchmark and publishes results", benchmark_only)

# Very experimental Docker pipeline. Should only be used with great care and is also not supported on all platforms.
Pipeline("docker",
         "Runs a benchmark against the official OpenSearch Docker container and publishes results", docker, stable=False)


def available_pipelines():
    return [[pipeline.name, pipeline.description] for pipeline in pipelines.values() if pipeline.stable]


def list_pipelines():
    console.println("Available pipelines:\n")
    console.println(tabulate.tabulate(available_pipelines(), headers=["Name", "Description"]))


def run(cfg):
    logger = logging.getLogger(__name__)
    # pipeline is no more mandatory, will default to benchmark-only
    name = cfg.opts("test_execution", "pipeline", mandatory=False)
    test_execution_id = cfg.opts("system", "test_execution.id")
    logger.info("Test Execution id [%s]", test_execution_id)
    if not name:
        # assume from-distribution pipeline if distribution.version has been specified
        if cfg.exists("builder", "distribution.version"):
            name = "from-distribution"
        else:
            name = "benchmark-only"
            logger.info("User did not specify distribution.version or pipeline. Using default pipeline [%s].", name)

        cfg.add(config.Scope.applicationOverride, "test_execution", "pipeline", name)
    else:
        logger.info("User specified pipeline [%s].", name)

    if os.environ.get("BENCHMARK_RUNNING_IN_DOCKER", "").upper() == "TRUE":
        # in this case only benchmarking remote OpenSearch clusters makes sense
        if name != "benchmark-only":
            raise exceptions.SystemSetupError(
                "Only the [benchmark-only] pipeline is supported by the OSB Docker image.\n"
                "Add --pipeline=benchmark-only in your OSB arguments and try again.\n"
                "For more details read the docs for the benchmark-only pipeline in {}\n".format(
                    doc_link("")))

    try:
        pipeline = pipelines[name]
    except KeyError:
        raise exceptions.SystemSetupError(
            "Unknown pipeline [%s]. List the available pipelines with %s list pipelines." % (name, PROGRAM_NAME))
    try:
        pipeline(cfg)
    except exceptions.BenchmarkError as e:
        # just pass on our own errors. It should be treated differently on top-level
        raise e
    except KeyboardInterrupt:
        logger.info("User has cancelled the benchmark.")
    except BaseException:
        tb = sys.exc_info()[2]
        raise exceptions.BenchmarkError("This test_execution ended with a fatal crash.").with_traceback(tb)
