import time

import pytest

import pyspark_spy


@pytest.fixture(scope='module', autouse=True)
def spark_job(sc, listener):
    sc.range(1, 100).count()
    time.sleep(1)


def test_job_recorded(listener):
    assert len(listener.python_events['jobEnd']) == 1


def test_stage_recorded(listener):
    assert len(listener.python_events['stageCompleted']) == 1


@pytest.mark.parametrize('field', [
    'jobId',
    'time',
    'jobResult'
])
def test_job_field_not_none(listener, field):
    job = listener.python_events['jobEnd'][0]
    assert getattr(job, field, None) is not None


def test_stage_info_not_none(listener):
    assert listener.python_events['stageCompleted'][0].stageInfo is not None


@pytest.mark.parametrize('field', [
    'name',
    'numTasks',
    'stageId',
    'attemptNumber',
    'submissionTime',
    'completionTime',
    'taskMetrics',
])
def test_stage_info_field_not_none(listener, field):
    stageInfo = listener.python_events['stageCompleted'][0].stageInfo
    assert getattr(stageInfo, field, None) is not None


def test_stage_info_num_tasks(listener):
    assert listener.python_events['stageCompleted'][0].stageInfo.numTasks == 1


@pytest.mark.parametrize('field', [
    'diskBytesSpilled',
    'executorCpuTime',
    'executorDeserializeCpuTime',
    'executorDeserializeTime',
    'executorRunTime',
    'jvmGCTime',
    'memoryBytesSpilled',
    'peakExecutionMemory',
    'resultSerializationTime',
    'resultSize',
])
def test_task_metrics_field_not_none(listener, field):
    taskMetrics = listener.python_events['stageCompleted'][0].stageInfo.taskMetrics
    assert getattr(taskMetrics, field, None) is not None
