from unittest.mock import patch, Mock
import pytest

from osbenchmark import config
from osbenchmark.aggregator import Aggregator

@pytest.fixture
def mock_config():
    return Mock(spec=config.Config)

@pytest.fixture
def mock_test_executions():
    return {
        "test1": Mock(),
        "test2": Mock()
    }

@pytest.fixture
def aggregator(mock_config, mock_test_executions):
    return Aggregator(mock_config, mock_test_executions)

def test_iterations(aggregator):
    mock_workload = Mock()
    mock_task = Mock()
    mock_operation = Mock(name="op1", iterations=5)
    mock_task.schedule = [mock_operation]
    mock_workload.test_procedures = [mock_task]

    with patch('osbenchmark.workload.load_workload', return_value=mock_workload):
        aggregator.iterations()

    assert aggregator.accumulated_iterations == {mock_operation.name: 5}

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

    aggregator.results(mock_test_execution)

    assert "task1" in aggregator.accumulated_results
    assert all(metric in aggregator.accumulated_results["task1"] for metric in
               ["throughput", "latency", "service_time", "client_processing_time",
                "processing_time", "error_rate", "duration"])

def test_aggregate_json_by_key(aggregator):
    mock_test_store = Mock()
    mock_test_store.find_by_test_execution_id.side_effect = [
        Mock(results={"key1": {"nested": 10}}),
        Mock(results={"key1": {"nested": 20}})
    ]

    with patch('osbenchmark.metrics.test_execution_store', return_value=mock_test_store):
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
    mock_test_store = Mock()
    mock_test_store.find_by_test_execution_id.side_effect = [
        Mock(workload="workload1"),
        Mock(workload="workload1"),
        Mock(workload="workload1")
    ]

    assert aggregator.compatibility_check(mock_test_store)

def test_compatibility_check_incompatible(aggregator):
    mock_test_store = Mock()
    mock_test_store.find_by_test_execution_id.side_effect = [
        Mock(workload="workload1"),
        Mock(workload="workload2"),
        Mock(workload="workload1")
    ]

    with pytest.raises(ValueError):
        aggregator.compatibility_check(mock_test_store)
