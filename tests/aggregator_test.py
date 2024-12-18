from unittest.mock import Mock
import pytest
from osbenchmark import config
from osbenchmark.aggregator import Aggregator, AggregatedResults

@pytest.fixture
def mock_config():
    mock_cfg = Mock(spec=config.Config)
    mock_cfg.opts.side_effect = lambda *args: "test_procedure_name" if args == ("workload", "test_procedure.name") else "/path/to/root"
    return mock_cfg

@pytest.fixture
def mock_test_executions():
    return {
        "test1": Mock(),
        "test2": Mock()
    }

@pytest.fixture
def mock_args():
    return Mock(
        results_file="",
        test_execution_id="",
        workload_repository="default"
    )

@pytest.fixture
def mock_test_store():
    mock_store = Mock()
    mock_store.find_by_test_execution_id.side_effect = [
        Mock(results={"key1": {"nested": 10}}, workload="workload1", test_procedure="test_proc1"),
        Mock(results={"key1": {"nested": 20}}, workload="workload1", test_procedure="test_proc1")
    ]
    return mock_store

@pytest.fixture
def aggregator(mock_config, mock_test_executions, mock_args, mock_test_store):
    aggregator = Aggregator(mock_config, mock_test_executions, mock_args)
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
    mock_test_execution = Mock(test_execution_id="test1", workload_params={})

    aggregator.loaded_workload = mock_workload
    aggregator.test_procedure_name = "test_procedure_name"

    aggregator.count_iterations_for_each_op(mock_test_execution)

    assert "test1" in aggregator.accumulated_iterations, "test1 not found in accumulated_iterations"
    assert "op1" in aggregator.accumulated_iterations["test1"], "op1 not found in accumulated_iterations for test1"
    assert aggregator.accumulated_iterations["test1"]["op1"] == 5

def test_accumulate_results(aggregator):
    mock_test_execution = Mock()
    mock_test_execution.results = {
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

    aggregator.accumulate_results(mock_test_execution)

    assert "task1" in aggregator.accumulated_results
    assert all(metric in aggregator.accumulated_results["task1"] for metric in aggregator.metrics)

def test_test_execution_compatibility_check(aggregator):
    mock_test_store = Mock()
    mock_test_store.find_by_test_execution_id.side_effect = [
        Mock(workload="workload1", test_procedure="test_proc1"),
        Mock(workload="workload1", test_procedure="test_proc1"),
        Mock(workload="workload1", test_procedure="test_proc1"),  # Add one more mock response
    ]
    aggregator.test_store = mock_test_store
    aggregator.test_executions = {"test1": Mock(), "test2": Mock()}

    assert aggregator.test_execution_compatibility_check()

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
    aggregator.test_executions = {"test1": Mock(), "test2": Mock()}

    result = aggregator.calculate_weighted_average(task_metrics, task_name)

    assert result["throughput"] == 160  # (100*2 + 200*3) / (2+3)
    assert result["latency"]["avg"] == 16  # (10*2 + 20*3) / (2+3)
    assert result["latency"]["unit"] == "ms"

def test_calculate_rsd(aggregator):
    values = [1, 2, 3, 4, 5]
    rsd = aggregator.calculate_rsd(values, "test_metric")
    assert isinstance(rsd, float)

def test_test_execution_compatibility_check_incompatible(aggregator):
    mock_test_store = Mock()
    mock_test_store.find_by_test_execution_id.side_effect = [
        Mock(workload="workload1", test_procedure="test_proc1"),
        Mock(workload="workload2", test_procedure="test_proc1"),
    ]
    aggregator.test_store = mock_test_store
    aggregator.test_executions = {"test1": Mock(), "test2": Mock()}
    with pytest.raises(ValueError):
        aggregator.test_execution_compatibility_check()

def test_aggregated_results():
    results = {"key": "value"}
    agg_results = AggregatedResults(results)
    assert agg_results.as_dict() == results
