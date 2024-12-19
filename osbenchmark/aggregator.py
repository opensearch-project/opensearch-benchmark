import os
import statistics
from typing import Any, Dict, List, Union
import uuid

from osbenchmark.metrics import FileTestExecutionStore, TestExecution
from osbenchmark import metrics, workload, config
from osbenchmark.utils import io as rio

class Aggregator:
    def __init__(self, cfg, test_executions_dict, args) -> None:
        self.config = cfg
        self.args = args
        self.test_executions = test_executions_dict
        self.accumulated_results: Dict[str, Dict[str, List[Any]]] = {}
        self.accumulated_iterations: Dict[str, Dict[str, int]] = {}
        self.metrics = ["throughput", "latency", "service_time", "client_processing_time", "processing_time", "error_rate", "duration"]
        self.test_store = metrics.test_execution_store(self.config)
        self.cwd = cfg.opts("node", "benchmark.cwd")
        self.test_execution = None
        self.test_procedure_name = None
        self.loaded_workload = None

    def count_iterations_for_each_op(self, test_execution: TestExecution) -> None:
        """Count iterations for each operation in the test execution"""
        workload_params = test_execution.workload_params if test_execution.workload_params else {}
        test_execution_id = test_execution.test_execution_id
        self.accumulated_iterations[test_execution_id] = {}

        for task in self.loaded_workload.find_test_procedure_or_default(self.test_procedure_name).schedule:
            task_name = task.name
            task_name_iterations = f"{task_name}_iterations"
            iterations = int(workload_params.get(task_name_iterations, task.iterations or 1))
            self.accumulated_iterations[test_execution_id][task_name] = iterations

    def accumulate_results(self, test_execution: TestExecution) -> None:
        """Accumulate results from a single test execution"""
        for operation_metric in test_execution.results.get("op_metrics", []):
            task = operation_metric.get("task", "")
            self.accumulated_results.setdefault(task, {})
            for metric in self.metrics:
                self.accumulated_results[task].setdefault(metric, [])
                self.accumulated_results[task][metric].append(operation_metric.get(metric))

    def aggregate_json_by_key(self, key_path: Union[str, List[str]]) -> Any:
        """
        Aggregates JSON results across multiple test executions using a specified key path.
        Handles nested dictionary structures and calculates averages for numeric values
        """
        all_json_results = [self.test_store.find_by_test_execution_id(id).results for id in self.test_executions.keys()]

        def get_nested_value(json_data: Dict[str, Any], path: List[str]) -> Any:
            """
            Retrieves a value from a nested dictionary structure using a path of keys.
            """
            for key in path:
                if isinstance(json_data, dict):
                    json_data = json_data.get(key, {})
                elif isinstance(json_data, list) and key.isdigit():
                    json_data = json_data[int(key)] if int(key) < len(json_data) else {}
                else:
                    return None
            return json_data

        def aggregate_json_elements(json_elements: List[Any]) -> Any:
            if not json_elements:
                return None
            # If all elements are numbers, calculate the average
            if all(isinstance(obj, (int, float)) for obj in json_elements):
                return sum(json_elements) / len(json_elements)
            # If all elements are dictionaries, recursively aggregate their values
            if all(isinstance(obj, dict) for obj in json_elements):
                keys = set().union(*json_elements)
                return {key: aggregate_json_elements([obj.get(key) for obj in json_elements]) for key in keys}
            # If all elements are lists, recursively aggregate corresponding elements
            if all(isinstance(obj, list) for obj in json_elements):
                max_length = max(len(obj) for obj in json_elements)
                return [aggregate_json_elements([obj[i] if i < len(obj) else None for obj in json_elements]) for i in range(max_length)]
            # If elements are of mixed types, return the first non-None value
            return next((obj for obj in json_elements if obj is not None), None)

        if isinstance(key_path, str):
            key_path = key_path.split('.')

        nested_values = [get_nested_value(json_result, key_path) for json_result in all_json_results]
        return aggregate_json_elements(nested_values)

    def build_aggregated_results_dict(self) -> Dict[str, Any]:
        """Builds a dictionary of aggregated metrics from all test executions"""
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
            aggregated_task_metrics = self.calculate_weighted_average(task_metrics, task)
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

        return aggregated_results

    def update_config_object(self, test_execution: TestExecution) -> None:
        """
        Updates the configuration object with values from a test execution.
        Uses the first test execution as reference since configurations should be identical
        """
        current_timestamp = self.config.opts("system", "time.start")
        self.config.add(config.Scope.applicationOverride, "builder",
                        "provision_config_instance.names", test_execution.provision_config_instance)
        self.config.add(config.Scope.applicationOverride, "system",
                        "env.name", test_execution.environment_name)
        self.config.add(config.Scope.applicationOverride, "system", "time.start", current_timestamp)
        self.config.add(config.Scope.applicationOverride, "test_execution", "pipeline", test_execution.pipeline)
        self.config.add(config.Scope.applicationOverride, "workload", "params", test_execution.workload_params)
        self.config.add(config.Scope.applicationOverride, "builder",
                        "provision_config_instance.params", test_execution.provision_config_instance_params)
        self.config.add(config.Scope.applicationOverride, "builder", "plugin.params", test_execution.plugin_params)
        self.config.add(config.Scope.applicationOverride, "workload", "latency.percentiles", test_execution.latency_percentiles)
        self.config.add(config.Scope.applicationOverride, "workload", "throughput.percentiles", test_execution.throughput_percentiles)

    def build_aggregated_results(self) -> TestExecution:
        test_exe = self.test_store.find_by_test_execution_id(list(self.test_executions.keys())[0])
        aggregated_results = self.build_aggregated_results_dict()

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

        self.update_config_object(test_exe)

        loaded_workload = workload.load_workload(self.config)
        test_procedure_object = loaded_workload.find_test_procedure_or_default(self.test_procedure_name)

        test_execution = metrics.create_test_execution(self.config, loaded_workload, test_procedure_object, test_exe.workload_revision)
        test_execution.user_tags = {
            "aggregation-of-runs": list(self.test_executions.keys())
        }
        test_execution.add_results(AggregatedResults(aggregated_results))
        test_execution.distribution_version = test_exe.distribution_version
        test_execution.revision = test_exe.revision
        test_execution.distribution_flavor = test_exe.distribution_flavor
        test_execution.provision_config_revision = test_exe.provision_config_revision

        return test_execution

    def calculate_weighted_average(self, task_metrics: Dict[str, List[Any]], task_name: str) -> Dict[str, Any]:
        weighted_metrics = {}

        # Get iterations for each test execution
        iterations_per_execution = [self.accumulated_iterations[test_id][task_name]
                                    for test_id in self.test_executions.keys()]
        total_iterations = sum(iterations_per_execution)

        for metric, values in task_metrics.items():
            if isinstance(values[0], dict):
                weighted_metrics[metric] = {}
                for metric_field in values[0].keys():
                    if metric_field == 'unit':
                        weighted_metrics[metric][metric_field] = values[0][metric_field]
                    elif metric_field == 'min':
                        weighted_metrics[metric]['overall_min'] = min(value.get(metric_field, 0) for value in values)
                    elif metric_field == 'max':
                        weighted_metrics[metric]['overall_max'] = max(value.get(metric_field, 0) for value in values)
                    else:
                        # for items like median or containing percentile values
                        item_values = [value.get(metric_field, 0) for value in values]
                        weighted_sum = sum(value * iterations for value, iterations in zip(item_values, iterations_per_execution))
                        weighted_metrics[metric][metric_field] = weighted_sum / total_iterations
            else:
                weighted_sum = sum(value * iterations for value, iterations in zip(values, iterations_per_execution))
                weighted_metrics[metric] = weighted_sum / total_iterations

        return weighted_metrics

    def calculate_rsd(self, values: List[Union[int, float]], metric_name: str) -> Union[float, str]:
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

        self.config.add(config.Scope.applicationOverride, "workload", "test_procedure.name", self.test_procedure_name)
        return True

    def aggregate(self) -> None:
        if self.test_execution_compatibility_check():
            self.test_execution = self.test_store.find_by_test_execution_id(list(self.test_executions.keys())[0])
            self.test_procedure_name = self.test_execution.test_procedure
            self.config.add(config.Scope.applicationOverride, "workload", "repository.name", self.args.workload_repository)
            self.config.add(config.Scope.applicationOverride, "workload", "workload.name", self.test_execution.workload)
            self.loaded_workload = workload.load_workload(self.config)
            for id in self.test_executions.keys():
                test_execution = self.test_store.find_by_test_execution_id(id)
                if test_execution:
                    self.count_iterations_for_each_op(test_execution)
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
