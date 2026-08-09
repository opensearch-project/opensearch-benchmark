from unittest.mock import Mock, patch
import pytest
from osbenchmark import config
from osbenchmark import metrics
from osbenchmark.aggregator import Aggregator, AggregatedResults

@pytest.fixture
def mock_config():
    mock_cfg = Mock(spec=config.Config)
    mock_cfg.opts.side_effect = lambda *args: "test_procedure_name" if args == ("workload", "test_procedure.name") else "/path/to/root"
    return mock_cfg

@pytest.fixture
def mock_test_runs():
    return {
        "test1": Mock(),
        "test2": Mock()
    }

@pytest.fixture
def mock_args():
    return Mock(
        results_file="",
        test_run_id="",
        workload_repository="default",
        workload_path=None
    )

@pytest.fixture
def mock_test_store():
    mock_store = Mock()
    mock_store.find_by_test_run_id.side_effect = [
        Mock(results={"key1": {"nested": 10}}, workload="workload1", test_procedure="test_proc1"),
        Mock(results={"key1": {"nested": 20}}, workload="workload1", test_procedure="test_proc1")
    ]
    return mock_store

@pytest.fixture
def aggregator(mock_config, mock_test_runs, mock_args, mock_test_store):
    aggregator = Aggregator(mock_config, mock_test_runs, mock_args)
    aggregator.test_store = mock_test_store
    return aggregator

def test_count_iterations_for_each_op(aggregator):
    mock_workload = Mock()
    mock_task = Mock(spec=['name', 'iterations'])
    mock_task.name = "op1"
    mock_task.iterations = 5
    mock_schedule = [mock_task]
    mock_test_procedure = Mock(spec=['name', 'schedule'])
    mock_test_procedure.name = "test_procedure_name"
    mock_test_procedure.schedule = mock_schedule
    mock_workload.test_procedures = [mock_test_procedure]

    mock_workload.find_test_procedure_or_default = Mock(return_value=mock_test_procedure)
    mock_test_run = Mock(test_run_id="test1", workload_params={})

    aggregator.loaded_workload = mock_workload
    aggregator.test_procedure_name = "test_procedure_name"

    aggregator.count_iterations_for_each_op(mock_test_run)

    assert "test1" in aggregator.accumulated_iterations, "test1 not found in accumulated_iterations"
    assert "op1" in aggregator.accumulated_iterations["test1"], "op1 not found in accumulated_iterations for test1"
    assert aggregator.accumulated_iterations["test1"]["op1"] == 5

def test_accumulate_results(aggregator):
    mock_test_run = Mock()
    mock_test_run.results = {
        "op_metrics": [
            {
                "task": "task1",
                "throughput": 100,
                "latency": {"avg": 10, "unit": "ms"},
                "service_time": 5,
                "client_processing_time": 2,
                "processing_time": 3,
                "error_rate": 0.1,
                "duration": 60
            }
        ]
    }

    aggregator.accumulate_results(mock_test_run)

    assert "task1" in aggregator.accumulated_results
    assert all(metric in aggregator.accumulated_results["task1"] for metric in aggregator.metrics)

def test_test_run_compatibility_check(aggregator):
    mock_test_store = Mock()
    mock_test_store.find_by_test_run_id.side_effect = [
        Mock(workload="workload1", test_procedure="test_proc1"),
        Mock(workload="workload1", test_procedure="test_proc1"),
        Mock(workload="workload1", test_procedure="test_proc1"),  # Add one more mock response
    ]
    aggregator.test_store = mock_test_store
    aggregator.test_runs = {"test1": Mock(), "test2": Mock()}

    assert aggregator.test_run_compatibility_check()

def test_aggregate_json_by_key(aggregator):
    result = aggregator.aggregate_json_by_key("key1.nested")
    assert result == 15

def test_calculate_weighted_average(aggregator):
    task_metrics = {
        "throughput": [100, 200],
        "latency": [{"avg": 10, "unit": "ms"}, {"avg": 20, "unit": "ms"}]
    }
    task_name = "op1"

    # set up accumulated_iterations
    aggregator.accumulated_iterations = {
        "test1": {"op1": 2},
        "test2": {"op1": 3}
    }
    aggregator.test_runs = {"test1": Mock(), "test2": Mock()}

    result = aggregator.calculate_weighted_average(task_metrics, task_name)

    assert result["throughput"] == 160  # (100*2 + 200*3) / (2+3)
    assert result["latency"]["avg"] == 16  # (10*2 + 20*3) / (2+3)
    assert result["latency"]["unit"] == "ms"

def test_update_config_object_reads_attributes_that_test_runs_actually_have(aggregator):
    # a real TestRun, not a Mock: a Mock creates whatever attribute is asked of it, so it cannot
    # tell us whether update_config_object reads attributes that a test run really carries
    test_run = metrics.TestRun(
        benchmark_version="1.0.0", benchmark_revision="abc123", environment_name="unit-test",
        test_run_id="test1", test_run_timestamp="20250101T000000Z", pipeline="benchmark-only",
        user_tags={}, workload="workload1", workload_params={}, test_procedure="test_proc1",
        cluster_config=["external"], cluster_config_params={"heap_size": "6g"}, plugin_params={},
        meta_data={})

    aggregator.update_config_object(test_run)

    aggregator.config.add.assert_any_call(config.Scope.applicationOverride, "builder",
                                          "cluster_config.params", {"heap_size": "6g"})

def test_calculate_weighted_average_with_null_metric_fields(aggregator):
    # An operation that produced no valid samples reports null metric fields, e.g. `optimize`
    # in the geonames workload, which reports error_rate 1.0 and a fully null throughput.
    task_metrics = {
        "throughput": [
            {"min": None, "mean": None, "median": None, "max": None, "unit": "ops/s"},
            {"min": None, "mean": None, "median": None, "max": None, "unit": "ops/s"}
        ]
    }
    aggregator.accumulated_iterations = {"test1": {"op1": 1}, "test2": {"op1": 1}}
    aggregator.test_runs = {"test1": Mock(), "test2": Mock()}

    result = aggregator.calculate_weighted_average(task_metrics, "op1")

    # null is carried through rather than replaced by 0, which would read as a real
    # measurement of zero throughput
    assert result["throughput"]["overall_min"] is None
    assert result["throughput"]["overall_max"] is None
    assert result["throughput"]["mean"] is None
    assert result["throughput"]["median"] is None
    assert result["throughput"]["unit"] == "ops/s"

def test_calculate_weighted_average_with_partially_null_metric_fields(aggregator):
    # A metric that has samples in one test run but not another: the valid values are
    # still aggregated, weighted only by the runs that contributed them.
    task_metrics = {
        "throughput": [
            {"min": 10, "mean": 20, "median": 20, "max": 30, "unit": "ops/s"},
            {"min": None, "mean": None, "median": None, "max": None, "unit": "ops/s"}
        ]
    }
    aggregator.accumulated_iterations = {"test1": {"op1": 2}, "test2": {"op1": 3}}
    aggregator.test_runs = {"test1": Mock(), "test2": Mock()}

    result = aggregator.calculate_weighted_average(task_metrics, "op1")

    assert result["throughput"]["overall_min"] == 10
    assert result["throughput"]["overall_max"] == 30
    assert result["throughput"]["mean"] == 20
    assert result["throughput"]["unit"] == "ops/s"
def _stub_run_for_aggregate(aggregator):
    aggregator.test_store.find_by_test_run_id.side_effect = None
    aggregator.test_store.find_by_test_run_id.return_value = Mock(
        results={}, workload="workload1", test_procedure="test_proc1")

def test_aggregate_names_the_workload_repository(aggregator):
    _stub_run_for_aggregate(aggregator)

    with patch("osbenchmark.workload.load_workload"), patch.object(aggregator, "build_aggregated_results"), \
            patch("osbenchmark.aggregator.FileTestRunStore"):
        aggregator.aggregate()

    aggregator.config.add.assert_any_call(config.Scope.applicationOverride, "workload",
                                          "repository.name", "default")

def test_aggregate_leaves_a_workload_path_alone(aggregator):
    # a workload passed as a path is already configured; also naming a repository would send the
    # loader hunting for the workload inside that repository instead
    aggregator.args.workload_path = "/path/to/my/workload"
    _stub_run_for_aggregate(aggregator)

    with patch("osbenchmark.workload.load_workload"), patch.object(aggregator, "build_aggregated_results"), \
            patch("osbenchmark.aggregator.FileTestRunStore"):
        aggregator.aggregate()

    repository_calls = [c for c in aggregator.config.add.call_args_list
                        if c.args[1:3] == ("workload", "repository.name")]
    assert repository_calls == []

def test_calculate_rsd(aggregator):
    values = [1, 2, 3, 4, 5]
    rsd = aggregator.calculate_rsd(values, "test_metric")
    assert isinstance(rsd, float)

def test_test_run_compatibility_check_incompatible(aggregator):
    mock_test_store = Mock()
    mock_test_store.find_by_test_run_id.side_effect = [
        Mock(workload="workload1", test_procedure="test_proc1"),
        Mock(workload="workload2", test_procedure="test_proc1"),
    ]
    aggregator.test_store = mock_test_store
    aggregator.test_runs = {"test1": Mock(), "test2": Mock()}
    with pytest.raises(ValueError):
        aggregator.test_run_compatibility_check()

def test_aggregated_results():
    results = {"key": "value"}
    agg_results = AggregatedResults(results)
    assert agg_results.as_dict() == results
