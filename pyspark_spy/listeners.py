import collections
import logging
from typing import List

from pyspark import SparkContext

from pyspark_spy.interface import SparkListener
from pyspark_spy.classes import JobEndEvent, StageCompletedEvent, OutputMetrics, InputMetrics

logger = logging.getLogger(__name__)


# noinspection PyPep8Naming,PyPep8Naming
class PersistingSparkListener(SparkListener):
    def __init__(self):
        self.java_events = collections.defaultdict(list)
        self.python_events = collections.defaultdict(list)

    def on_spark_event(self, event_name, java_event):
        self.java_events[event_name].append(java_event)
        try:
            self.python_events[event_name].append(self.from_java_event(event_name, java_event))
        except:
            logger.error('Error converting %s from java to python', event_name)

    def from_java_event(self, event_name, java_event):
        if event_name == 'jobEnd':
            return JobEndEvent.from_java(java_event)
        elif event_name == 'stageCompleted':
            return StageCompletedEvent.from_java(java_event)

    @property
    def applicationEnd(self) -> List:
        return self.python_events['applicationEnd']

    @property
    def applicationStart(self) -> List:
        return self.python_events['applicationStart']

    @property
    def blockManagerRemoved(self) -> List:
        return self.python_events['blockManagerRemoved']

    @property
    def blockUpdated(self) -> List:
        return self.python_events['blockUpdated']

    @property
    def environmentUpdate(self) -> List:
        return self.python_events['environmentUpdate']

    @property
    def executorAdded(self) -> List:
        return self.python_events['executorAdded']

    @property
    def executorMetricsUpdate(self) -> List:
        return self.python_events['executorMetricsUpdate']

    @property
    def executorRemoved(self) -> List:
        return self.python_events['executorRemoved']

    @property
    def jobEnd(self) -> List[JobEndEvent]:
        return self.python_events['jobEnd']

    @property
    def jobStart(self) -> List:
        return self.python_events['jobStart']

    @property
    def otherEvent(self) -> List:
        return self.python_events['otherEvent']

    @property
    def stageCompleted(self) -> List[StageCompletedEvent]:
        return self.python_events['stageCompleted']

    @property
    def stageSubmitted(self) -> List:
        return self.python_events['stageSubmitted']

    @property
    def taskEnd(self) -> List:
        return self.python_events['taskEnd']

    @property
    def taskGettingResult(self) -> List:
        return self.python_events['taskGettingResult']

    @property
    def taskStart(self) -> List:
        return self.python_events['taskStart']

    @property
    def unpersistRDD(self) -> List:
        return self.python_events['unpersistRDD']

    def stage_output_metrics_aggregate(self) -> OutputMetrics:
        # noinspection PyArgumentList
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
        # noinspection PyArgumentList
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


class ContextSparkListener(PersistingSparkListener):
    def __init__(self):
        super(ContextSparkListener, self).__init__()
        self.listeners = []  # type: List[PersistingSparkListener]

    def __enter__(self):
        listener = PersistingSparkListener()
        self.listeners.append(listener)
        return listener

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.listeners.pop()

    def on_spark_event(self, event_name, java_event):
        for listener in self.listeners:
            listener.on_spark_event(event_name, java_event)

        super(ContextSparkListener, self).on_spark_event(event_name, java_event)


class LoggingSparkListener(SparkListener):
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def on_spark_event(self, event_name, java_event):
        self.logger.debug('SparkListener event received: %s', event_name)


class StdoutSparkListener(LoggingSparkListener):
    def __init__(self):
        super(StdoutSparkListener, self).__init__()
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)


def register_listener(sc: SparkContext, *listeners: SparkListener):
    callback_server_started = sc._gateway.start_callback_server()
    if callback_server_started:
        logger.debug('Callback server started')

    for listener in listeners:
        sc._jsc.sc().addSparkListener(listener)
