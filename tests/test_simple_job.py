import pytest

import pyspark_spy


@pytest.fixture(scope='module')
def simple_job_events(sc):
    with pyspark_spy.capture_spark_events(sc) as events:
        sc.range(1, 100).count()
    return events


def test_job_recorded(simple_job_events):
    assert len(simple_job_events.jobEnd) == 1


def test_stage_recorded(simple_job_events):
    assert len(simple_job_events.stageCompleted) == 1


@pytest.mark.parametrize('field', [
    'jobId',
    'time',
    'jobResult'
])
def test_job_field_not_none(simple_job_events, field):
    job = simple_job_events.jobEnd[0]
    assert getattr(job, field, None) is not None


def test_stage_info_not_none(simple_job_events):
    assert simple_job_events.stageCompleted[0].stageInfo is not None


@pytest.mark.parametrize('field', [
    'name',
    'numTasks',
    'stageId',
    'attemptNumber',
    'submissionTime',
    'completionTime',
    'taskMetrics',
])
def test_stage_info_field_not_none(simple_job_events, field):
    stageInfo = simple_job_events.stageCompleted[0].stageInfo
    assert getattr(stageInfo, field, None) is not None


def test_stage_info_num_tasks(simple_job_events):
    assert simple_job_events.stageCompleted[0].stageInfo.numTasks == 1


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
def test_task_metrics_field_not_none(simple_job_events, field):
    taskMetrics = simple_job_events.stageCompleted[0].stageInfo.taskMetrics
    assert getattr(taskMetrics, field, None) is not None
