import os
import statistics
from typing import Any, Dict, List, Union
import uuid

from osbenchmark.metrics import FileTestExecutionStore
from osbenchmark import metrics, workload, config
from osbenchmark.utils import io as rio

class Aggregator:
    def __init__(self, cfg, test_executions_dict, args):
        self.config = cfg
        self.args = args
        self.test_executions = test_executions_dict
        self.accumulated_results: Dict[str, Dict[str, List[Any]]] = {}
        self.accumulated_iterations: Dict[str, int] = {}
        self.metrics = ["throughput", "latency", "service_time", "client_processing_time", "processing_time", "error_rate", "duration"]
        self.test_store = metrics.test_execution_store(self.config)
        self.cwd = cfg.opts("node", "benchmark.cwd")

    def count_iterations_for_each_op(self) -> None:
        loaded_workload = workload.load_workload(self.config)
        test_procedure_name = self.config.opts("workload", "test_procedure.name")
        test_procedure_found = False

        for test_procedure in loaded_workload.test_procedures:
            if test_procedure.name == test_procedure_name:
                test_procedure_found = True
                for task in test_procedure.schedule:
                    task_name = task.name
                    iterations = task.iterations or 1
                    self.accumulated_iterations[task_name] = self.accumulated_iterations.get(task_name, 0) + iterations
            else:
                continue  # skip to the next test procedure if the name doesn't match

        if not test_procedure_found:
            raise ValueError(f"Test procedure '{test_procedure_name}' not found in the loaded workload.")

    def accumulate_results(self, test_execution: Any) -> None:
        for item in test_execution.results.get("op_metrics", []):
            task = item.get("task", "")
            self.accumulated_results.setdefault(task, {})
            for metric in self.metrics:
                self.accumulated_results[task].setdefault(metric, [])
                self.accumulated_results[task][metric].append(item.get(metric))

    def aggregate_json_by_key(self, key_path: Union[str, List[str]]) -> Any:
        all_jsons = [self.test_store.find_by_test_execution_id(id).results for id in self.test_executions.keys()]

        # retrieve nested value from a dictionary given a key path
        def get_nested_value(obj: Dict[str, Any], path: List[str]) -> Any:
            for key in path:
                if isinstance(obj, dict):
                    obj = obj.get(key, {})
                elif isinstance(obj, list) and key.isdigit():
                    obj = obj[int(key)] if int(key) < len(obj) else {}
                else:
                    return None
            return obj

        def aggregate_helper(objects: List[Any]) -> Any:
            if not objects:
                return None
            if all(isinstance(obj, (int, float)) for obj in objects):
                avg = sum(objects) / len(objects)
                return avg
            if all(isinstance(obj, dict) for obj in objects):
                keys = set().union(*objects)
                return {key: aggregate_helper([obj.get(key) for obj in objects]) for key in keys}
            if all(isinstance(obj, list) for obj in objects):
                max_length = max(len(obj) for obj in objects)
                return [aggregate_helper([obj[i] if i < len(obj) else None for obj in objects]) for i in range(max_length)]
            return next((obj for obj in objects if obj is not None), None)

        if isinstance(key_path, str):
            key_path = key_path.split('.')

        values = [get_nested_value(json, key_path) for json in all_jsons]
        return aggregate_helper(values)

    def build_aggregated_results(self):
        test_exe = self.test_store.find_by_test_execution_id(list(self.test_executions.keys())[0])
        aggregated_results = {
            "op_metrics": [],
            "correctness_metrics": self.aggregate_json_by_key("correctness_metrics"),
            "total_time": self.aggregate_json_by_key("total_time"),
            "total_time_per_shard": self.aggregate_json_by_key("total_time_per_shard"),
            "indexing_throttle_time": self.aggregate_json_by_key("indexing_throttle_time"),
            "indexing_throttle_time_per_shard": self.aggregate_json_by_key("indexing_throttle_time_per_shard"),
            "merge_time": self.aggregate_json_by_key("merge_time"),
            "merge_time_per_shard": self.aggregate_json_by_key("merge_time_per_shard"),
            "merge_count": self.aggregate_json_by_key("merge_count"),
            "refresh_time": self.aggregate_json_by_key("refresh_time"),
            "refresh_time_per_shard": self.aggregate_json_by_key("refresh_time_per_shard"),
            "refresh_count": self.aggregate_json_by_key("refresh_count"),
            "flush_time": self.aggregate_json_by_key("flush_time"),
            "flush_time_per_shard": self.aggregate_json_by_key("flush_time_per_shard"),
            "flush_count": self.aggregate_json_by_key("flush_count"),
            "merge_throttle_time": self.aggregate_json_by_key("merge_throttle_time"),
            "merge_throttle_time_per_shard": self.aggregate_json_by_key("merge_throttle_time_per_shard"),
            "ml_processing_time": self.aggregate_json_by_key("ml_processing_time"),
            "young_gc_time": self.aggregate_json_by_key("young_gc_time"),
            "young_gc_count": self.aggregate_json_by_key("young_gc_count"),
            "old_gc_time": self.aggregate_json_by_key("old_gc_time"),
            "old_gc_count": self.aggregate_json_by_key("old_gc_count"),
            "memory_segments": self.aggregate_json_by_key("memory_segments"),
            "memory_doc_values": self.aggregate_json_by_key("memory_doc_values"),
            "memory_terms": self.aggregate_json_by_key("memory_terms"),
            "memory_norms": self.aggregate_json_by_key("memory_norms"),
            "memory_points": self.aggregate_json_by_key("memory_points"),
            "memory_stored_fields": self.aggregate_json_by_key("memory_stored_fields"),
            "store_size": self.aggregate_json_by_key("store_size"),
            "translog_size": self.aggregate_json_by_key("translog_size"),
            "segment_count": self.aggregate_json_by_key("segment_count"),
            "total_transform_search_times": self.aggregate_json_by_key("total_transform_search_times"),
            "total_transform_index_times": self.aggregate_json_by_key("total_transform_index_times"),
            "total_transform_processing_times": self.aggregate_json_by_key("total_transform_processing_times"),
            "total_transform_throughput": self.aggregate_json_by_key("total_transform_throughput")
        }

        for task, task_metrics in self.accumulated_results.items():
            iterations = self.accumulated_iterations.get(task, 1)
            aggregated_task_metrics = self.calculate_weighted_average(task_metrics, iterations)
            op_metric = {
                "task": task,
                "operation": task,
            }
            for metric in self.metrics:
                op_metric[metric] = aggregated_task_metrics[metric]

                # Handle standard metrics (like latency, service_time) which are stored as dictionaries
                if isinstance(aggregated_task_metrics[metric], dict):
                    # Calculate RSD for the mean values across all test executions
                    # We use mean here as it's more sensitive to outliers, which is desirable for assessing variability
                    mean_values = [v['mean'] for v in task_metrics[metric]]
                    rsd = self.calculate_rsd(mean_values, f"{task}.{metric}.mean")
                    op_metric[metric]['mean_rsd'] = rsd

                # Handle derived metrics (like error_rate, duration) which are stored as simple values
                else:
                    # Calculate RSD directly from the metric values across all test executions
                    rsd = self.calculate_rsd(task_metrics[metric], f"{task}.{metric}")
                    op_metric[f"{metric}_rsd"] = rsd

            aggregated_results["op_metrics"].append(op_metric)

        # extract the necessary data from the first test execution, since the configurations should be identical for all test executions
        current_timestamp = self.config.opts("system", "time.start")

        if hasattr(self.args, 'results_file') and self.args.results_file != "":
            normalized_results_file = rio.normalize_path(self.args.results_file, self.cwd)
            # ensure that the parent folder already exists when we try to write the file...
            rio.ensure_dir(rio.dirname(normalized_results_file))
            test_execution_id = os.path.basename(normalized_results_file)
            self.config.add(config.Scope.applicationOverride, "system", "test_execution.id", normalized_results_file)
        elif hasattr(self.args, 'test_execution_id') and self.args.test_execution_id:
            test_execution_id = f"aggregate_results_{test_exe.workload}_{self.args.test_execution_id}"
            self.config.add(config.Scope.applicationOverride, "system", "test_execution.id", test_execution_id)
        else:
            test_execution_id = f"aggregate_results_{test_exe.workload}_{str(uuid.uuid4())}"
            self.config.add(config.Scope.applicationOverride, "system", "test_execution.id", test_execution_id)

        print("Aggregate test execution ID: ", test_execution_id)

        # add values to the configuration object
        self.config.add(config.Scope.applicationOverride, "builder",
                        "provision_config_instance.names", test_exe.provision_config_instance)
        self.config.add(config.Scope.applicationOverride, "system",
                        "env.name", test_exe.environment_name)
        self.config.add(config.Scope.applicationOverride, "system", "time.start", current_timestamp)
        self.config.add(config.Scope.applicationOverride, "test_execution", "pipeline", test_exe.pipeline)
        self.config.add(config.Scope.applicationOverride, "workload", "params", test_exe.workload_params)
        self.config.add(config.Scope.applicationOverride, "builder",
                        "provision_config_instance.params", test_exe.provision_config_instance_params)
        self.config.add(config.Scope.applicationOverride, "builder", "plugin.params", test_exe.plugin_params)
        self.config.add(config.Scope.applicationOverride, "workload", "latency.percentiles", test_exe.latency_percentiles)
        self.config.add(config.Scope.applicationOverride, "workload", "throughput.percentiles", test_exe.throughput_percentiles)

        loaded_workload = workload.load_workload(self.config)
        test_procedure = loaded_workload.find_test_procedure_or_default(test_exe.test_procedure)

        test_execution = metrics.create_test_execution(self.config, loaded_workload, test_procedure, test_exe.workload_revision)
        test_execution.user_tags = {
            "aggregation-of-runs": list(self.test_executions.keys())
        }
        test_execution.add_results(AggregatedResults(aggregated_results))
        test_execution.distribution_version = test_exe.distribution_version
        test_execution.revision = test_exe.revision
        test_execution.distribution_flavor = test_exe.distribution_flavor
        test_execution.provision_config_revision = test_exe.provision_config_revision

        return test_execution

    def calculate_weighted_average(self, task_metrics: Dict[str, List[Any]], iterations: int) -> Dict[str, Any]:
        weighted_metrics = {}

        for metric, values in task_metrics.items():
            if isinstance(values[0], dict):
                weighted_metrics[metric] = {}
                for item_key in values[0].keys():
                    if item_key == 'unit':
                        weighted_metrics[metric][item_key] = values[0][item_key]
                    else:
                        item_values = [value.get(item_key, 0) for value in values]
                        weighted_sum = sum(value * iterations for value in item_values)
                        total_iterations = iterations * len(item_values)
                        weighted_avg = weighted_sum / total_iterations
                        weighted_metrics[metric][item_key] = weighted_avg
            else:
                weighted_sum = sum(value * iterations for value in values)
                total_iterations = iterations * len(values)
                weighted_avg = weighted_sum / total_iterations
                weighted_metrics[metric] = weighted_avg

        return weighted_metrics

    def calculate_rsd(self, values: List[Union[int, float]], metric_name: str):
        if not values:
            raise ValueError(f"Cannot calculate RSD for metric '{metric_name}': empty list of values")
        if len(values) == 1:
            return "NA"  # RSD is not applicable for a single value
        mean = statistics.mean(values)
        std_dev = statistics.stdev(values)
        return (std_dev / mean) * 100 if mean != 0 else float('inf')

    def test_execution_compatibility_check(self) -> None:
        first_test_execution = self.test_store.find_by_test_execution_id(list(self.test_executions.keys())[0])
        workload = first_test_execution.workload
        test_procedure = first_test_execution.test_procedure
        for id in self.test_executions.keys():
            test_execution = self.test_store.find_by_test_execution_id(id)
            if test_execution:
                if test_execution.workload != workload:
                    raise ValueError(
                        f"Incompatible workload: test {id} has workload '{test_execution.workload}' instead of '{workload}'. "
                        f"Ensure that all test IDs have the same workload."
                    )
                if test_execution.test_procedure != test_procedure:
                    raise ValueError(
                        f"Incompatible test procedure: test {id} has test procedure '{test_execution.test_procedure}' "
                        f"instead of '{test_procedure}'. Ensure that all test IDs have the same test procedure from the same workload."
                    )
            else:
                raise ValueError(f"Test execution not found: {id}. Ensure that all provided test IDs are valid.")

        self.config.add(config.Scope.applicationOverride, "workload", "test_procedure.name", first_test_execution.test_procedure)
        return True

    def aggregate(self) -> None:
        if self.test_execution_compatibility_check():
            for id in self.test_executions.keys():
                test_execution = self.test_store.find_by_test_execution_id(id)
                if test_execution:
                    self.config.add(config.Scope.applicationOverride, "workload", "repository.name", self.args.workload_repository)
                    self.config.add(config.Scope.applicationOverride, "workload", "workload.name", test_execution.workload)
                    self.count_iterations_for_each_op()
                    self.accumulate_results(test_execution)

            aggregated_results = self.build_aggregated_results()
            file_test_exe_store = FileTestExecutionStore(self.config)
            file_test_exe_store.store_aggregated_execution(aggregated_results)
        else:
            raise ValueError("Incompatible test execution results")

class AggregatedResults:
    def __init__(self, results):
        self.results = results

    def as_dict(self):
        return self.results
