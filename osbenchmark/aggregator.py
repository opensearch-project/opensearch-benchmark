from typing import Any, Dict, List, Union
import uuid

from osbenchmark.metrics import FileTestExecutionStore
from osbenchmark import metrics, workload, config

class Aggregator:
    def __init__(self, cfg, test_executions_dict):
        self.config = cfg
        self.test_executions = test_executions_dict
        self.accumulated_results: Dict[str, Dict[str, List[Any]]] = {}
        self.accumulated_iterations: Dict[str, int] = {}

    # count iterations for each operation in the workload
    def iterations(self) -> None:
        loaded_workload = workload.load_workload(self.config)
        for task in loaded_workload.test_procedures:
            for operation in task.schedule:
                operation_name = operation.name
                iterations = operation.iterations or 1
                self.accumulated_iterations.setdefault(operation_name, 0)
                self.accumulated_iterations[operation_name] += iterations

    # accumulate metrics for each task from test execution results
    def results(self, test_execution: Any) -> None:
        for item in test_execution.results.get("op_metrics", []):
            task = item.get("task", "")
            self.accumulated_results.setdefault(task, {})
            for metric in ["throughput", "latency", "service_time", "client_processing_time", "processing_time", "error_rate", "duration"]:
                self.accumulated_results[task].setdefault(metric, [])
                self.accumulated_results[task][metric].append(item.get(metric))

    # aggregate values from multiple test execution result JSON objects by a specified key path
    def aggregate_json_by_key(self, key_path: Union[str, List[str]]) -> Any:
        test_store = metrics.test_execution_store(self.config)
        all_jsons = [test_store.find_by_test_execution_id(id).results for id in self.test_executions.keys()]

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

        # recursively aggregate values, handling different data types
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

    # construct aggregated results dict
    def build_aggregated_results(self, test_store):
        test_exe = test_store.find_by_test_execution_id(list(self.test_executions.keys())[0])
        aggregated_results = {
            "op-metrics": [],
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
                "throughput": aggregated_task_metrics["throughput"],
                "latency": aggregated_task_metrics["latency"],
                "service_time": aggregated_task_metrics["service_time"],
                "client_processing_time": aggregated_task_metrics["client_processing_time"],
                "processing_time": aggregated_task_metrics["processing_time"],
                "error_rate": aggregated_task_metrics["error_rate"],
                "duration": aggregated_task_metrics["duration"]
            }
            aggregated_results["op-metrics"].append(op_metric)

        # extract the necessary data from the first test execution, since the configurations should be identical for all test executions
        test_exe_store = metrics.test_execution_store(self.config)
        first_test_execution = test_exe_store.find_by_test_execution_id(list(self.test_executions.keys())[0])
        current_timestamp = self.config.opts("system", "time.start")

        # add values to the configuration object
        self.config.add(config.Scope.applicationOverride, "builder",
                        "provision_config_instance.names", first_test_execution.provision_config_instance)
        self.config.add(config.Scope.applicationOverride, "system",
                        "env.name", first_test_execution.environment_name)
        self.config.add(config.Scope.applicationOverride, "system", "test_execution.id",
                        f"aggregate_results_{first_test_execution.workload}_{str(uuid.uuid4())}")
        self.config.add(config.Scope.applicationOverride, "system", "time.start", current_timestamp)
        self.config.add(config.Scope.applicationOverride, "test_execution", "pipeline", first_test_execution.pipeline)
        self.config.add(config.Scope.applicationOverride, "workload", "params", first_test_execution.workload_params)
        self.config.add(config.Scope.applicationOverride, "builder",
                        "provision_config_instance.params", first_test_execution.provision_config_instance_params)
        self.config.add(config.Scope.applicationOverride, "builder", "plugin.params", first_test_execution.plugin_params)
        self.config.add(config.Scope.applicationOverride, "workload", "latency.percentiles", first_test_execution.latency_percentiles)
        self.config.add(config.Scope.applicationOverride, "workload", "throughput.percentiles", first_test_execution.throughput_percentiles)

        loaded_workload = workload.load_workload(self.config)
        test_procedure = loaded_workload.find_test_procedure_or_default(first_test_execution.test_procedure)

        test_execution = metrics.create_test_execution(self.config, loaded_workload, test_procedure, first_test_execution.workload_revision)
        test_execution.add_results(aggregated_results)
        test_execution.distribution_version = test_exe.distribution_version
        test_execution.revision = test_exe.revision
        test_execution.distribution_flavor = test_exe.distribution_flavor
        test_execution.provision_config_revision = test_exe.provision_config_revision

        return test_execution

    # calculate weighted averages for task metrics
    def calculate_weighted_average(self, task_metrics: Dict[str, List[Any]], iterations: int) -> Dict[str, Any]:
        weighted_metrics = {}

        for metric, values in task_metrics.items():
            weighted_metrics[metric] = {}
            if isinstance(values[0], dict):
                for item_key in values[0].keys():
                    if item_key == 'unit':
                        weighted_metrics[metric][item_key] = values[0][item_key]
                    else:
                        item_values = [value.get(item_key, 0) for value in values]
                        if iterations > 1:
                            weighted_sum = sum(value * iterations for value in item_values)
                            total_iterations = iterations * len(values)
                            weighted_metrics[metric][item_key] = weighted_sum / total_iterations
                        else:
                            weighted_metrics[metric][item_key] = sum(item_values) / len(item_values)
            else:
                if iterations > 1:
                    weighted_sum = sum(value * iterations for value in values)
                    total_iterations = iterations * len(values)
                    weighted_metrics[metric] = weighted_sum / total_iterations
                else:
                    weighted_metrics[metric] = sum(values) / len(values)
        return weighted_metrics

    # verify that all test executions have the same workload
    def compatibility_check(self, test_store) -> None:
        first_test_execution = test_store.find_by_test_execution_id(list(self.test_executions.keys())[0])
        workload = first_test_execution.workload
        for id in self.test_executions.keys():
            test_execution = test_store.find_by_test_execution_id(id)
            if test_execution:
                if test_execution.workload != workload:
                    raise ValueError(f"Incompatible workload: test {id} has workload '{test_execution.workload}' instead of '{workload}'")
            else:
                raise ValueError("Test execution not found: ", id)
        return True

    # driver code
    def aggregate(self) -> None:
        test_execution_store = metrics.test_execution_store(self.config)
        if self.compatibility_check(test_execution_store):
            for id in self.test_executions.keys():
                test_execution = test_execution_store.find_by_test_execution_id(id)
                if test_execution:
                    self.config.add(config.Scope.applicationOverride, "workload", "repository.name", "default")
                    self.config.add(config.Scope.applicationOverride, "workload", "workload.name", test_execution.workload)
                    self.iterations()
                    self.results(test_execution)

            aggregated_results = self.build_aggregated_results(test_execution_store)
            file_test_exe_store = FileTestExecutionStore(self.config)
            file_test_exe_store.store_test_execution(aggregated_results)
        else:
            raise ValueError("Incompatible test execution results")
