from unittest.mock import patch, Mock
import pytest

from osbenchmark import config
from osbenchmark.aggregator import Aggregator

@pytest.fixture
def mock_config():
    mock_cfg = Mock(spec=config.Config)
    mock_cfg.opts.side_effect = lambda *args: "/path/to/root" if args == ("node", "root.dir") else None
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
        Mock(results={"key1": {"nested": 10}}),
        Mock(results={"key1": {"nested": 20}})
    ]
    return mock_store

@pytest.fixture
def aggregator(mock_config, mock_test_executions, mock_args, mock_test_store):
    aggregator = Aggregator(mock_config, mock_test_executions, mock_args)
    aggregator.test_store = mock_test_store
    return aggregator

def test_iterations(aggregator, mock_args):
    mock_workload = Mock()
    mock_schedule = [Mock(name="op1", iterations=5)]
    mock_task = Mock(name="task1", schedule=mock_schedule)
    mock_workload.test_procedures = [mock_task]

    # Mock the config.opts call to return the same test_procedure.name
    aggregator.config.opts.side_effect = lambda *args: mock_task.name if args == ("workload", "test_procedure.name") else None

    with patch('osbenchmark.workload.load_workload', return_value=mock_workload):
        aggregator.count_iterations_for_each_op()

    assert list(aggregator.accumulated_iterations.values())[0] == 5

def test_results(aggregator):
    mock_test_execution = Mock()
    mock_test_execution.results = {
        "op_metrics": [
            {
                "task": "task1",
                "throughput": 100,
                "latency": 10,
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
    assert all(metric in aggregator.accumulated_results["task1"] for metric in
               ["throughput", "latency", "service_time", "client_processing_time",
                "processing_time", "error_rate", "duration"])

def test_aggregate_json_by_key(aggregator):
    result = aggregator.aggregate_json_by_key("key1.nested")
    assert result == 15

def test_calculate_weighted_average(aggregator):
    task_metrics = {
        "throughput": [100, 200],
        "latency": [{"avg": 10, "unit": "ms"}, {"avg": 20, "unit": "ms"}]
    }
    iterations = 2

    result = aggregator.calculate_weighted_average(task_metrics, iterations)

    assert result["throughput"] == 150
    assert result["latency"]["avg"] == 15
    assert result["latency"]["unit"] == "ms"

def test_compatibility_check(aggregator):
    mock_test_procedure = Mock(name="test_procedure")
    mock_test_store = Mock()
    mock_test_store.find_by_test_execution_id.side_effect = [
        Mock(workload="workload1", test_procedure=mock_test_procedure),
        Mock(workload="workload1", test_procedure=mock_test_procedure),
        Mock(workload="workload1", test_procedure=mock_test_procedure)
    ]
    aggregator.test_store = mock_test_store
    assert aggregator.test_execution_compatibility_check()


def test_compatibility_check_incompatible(aggregator):
    mock_test_store = Mock()
    mock_test_store.find_by_test_execution_id.side_effect = [
        Mock(workload="workload1"),
        Mock(workload="workload2"),
        Mock(workload="workload1")
    ]
    aggregator.test_store = mock_test_store
    with pytest.raises(ValueError):
        aggregator.test_execution_compatibility_check()
