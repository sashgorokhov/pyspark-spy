from typing import List

from pyspark_spy.classes import JobEndEvent, StageCompletedEvent, OutputMetrics, InputMetrics
from pyspark_spy.interface import SparkListener


class RememberingSparkListener(SparkListener):
    def __init__(self):
        self.stop_recording = False

        self.jobEnd = []
        self.stageCompleted = []

    def onJobEnd(self, jobEnd):
        if not self.stop_recording:
            self.jobEnd.append(jobEnd)

    def onStageCompleted(self, stageCompleted):
        if not self.stop_recording:
            self.stageCompleted.append(stageCompleted)


class SparkEvents:
    """
    :param List[JobEndEvent] jobEnd:
    :param List[StageCompletedEvent] stageCompleted:
    """

    def __init__(self):
        self.jobEnd = []
        self.stageCompleted = []

    def parse_listener_events(self, listener: RememberingSparkListener):
        self.jobEnd = list(filter(None, map(JobEndEvent.from_java, listener.jobEnd)))
        self.stageCompleted = list(filter(None, map(StageCompletedEvent.from_java, listener.stageCompleted)))

    def stage_output_metrics_aggregate(self) -> OutputMetrics:
        return OutputMetrics(
            bytesWritten=sum(
                e.stageInfo.taskMetrics.outputMetrics.bytesWritten
                for e in self.stageCompleted
            ),
            recordsWritten=sum(
                e.stageInfo.taskMetrics.outputMetrics.recordsWritten
                for e in self.stageCompleted
            ),
        )

    def stage_input_metrics_aggregate(self) -> InputMetrics:
        return InputMetrics(
            bytesRead=sum(
                e.stageInfo.taskMetrics.inputMetrics.bytesRead
                for e in self.stageCompleted
            ),
            recordsRead=sum(
                e.stageInfo.taskMetrics.inputMetrics.recordsRead
                for e in self.stageCompleted
            ),
        )

    def __repr__(self):
        return '{cls.__name__}(jobEnd={jobEnd}, stageCompleted={stageCompleted})'.format(
            cls=self.__class__,
            jobEnd=self.jobEnd,
            stageCompleted=self.stageCompleted
        )
