# noinspection PyPep8Naming,SpellCheckingInspection
class SparkListenerInterface(object):
    """
    SparkListener python interface.

    https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/scheduler/SparkListener.html
    """

    def onApplicationEnd(self, applicationEnd):
        pass

    def onApplicationStart(self, applicationStart):
        pass

    def onBlockManagerRemoved(self, blockManagerRemoved):
        pass

    def onBlockUpdated(self, blockUpdated):
        pass

    def onEnvironmentUpdate(self, environmentUpdate):
        pass

    def onExecutorAdded(self, executorAdded):
        pass

    def onExecutorMetricsUpdate(self, executorMetricsUpdate):
        pass

    def onExecutorRemoved(self, executorRemoved):
        pass

    def onJobEnd(self, jobEnd):
        pass

    def onJobStart(self, jobStart):
        pass

    def onOtherEvent(self, event):
        pass

    def onStageCompleted(self, stageCompleted):
        pass

    def onStageSubmitted(self, stageSubmitted):
        pass

    def onTaskEnd(self, taskEnd):
        pass

    def onTaskGettingResult(self, taskGettingResult):
        pass

    def onTaskStart(self, taskStart):
        pass

    def onUnpersistRDD(self, unpersistRDD):
        pass

    class Java:
        implements = ["org.apache.spark.scheduler.SparkListenerInterface"]


# noinspection PyPep8Naming,SpellCheckingInspection
class SparkListener(SparkListenerInterface):
    def on_spark_event(self, event_name, java_event):
        raise NotImplementedError()

    def onApplicationEnd(self, applicationEnd):
        self.on_spark_event('applicationEnd', applicationEnd)

    def onApplicationStart(self, applicationStart):
        self.on_spark_event('applicationStart', applicationStart)

    def onBlockManagerRemoved(self, blockManagerRemoved):
        self.on_spark_event('blockManagerRemoved', blockManagerRemoved)

    def onBlockUpdated(self, blockUpdated):
        self.on_spark_event('blockUpdated', blockUpdated)

    def onEnvironmentUpdate(self, environmentUpdate):
        self.on_spark_event('environmentUpdate', environmentUpdate)

    def onExecutorAdded(self, executorAdded):
        self.on_spark_event('executorAdded', executorAdded)

    def onExecutorMetricsUpdate(self, executorMetricsUpdate):
        self.on_spark_event('executorMetricsUpdate', executorMetricsUpdate)

    def onExecutorRemoved(self, executorRemoved):
        self.on_spark_event('executorRemoved', executorRemoved)

    def onJobEnd(self, jobEnd):
        self.on_spark_event('jobEnd', jobEnd)

    def onJobStart(self, jobStart):
        self.on_spark_event('jobStart', jobStart)

    def onOtherEvent(self, otherEvent):
        self.on_spark_event('otherEvent', otherEvent)

    def onStageCompleted(self, stageCompleted):
        self.on_spark_event('stageCompleted', stageCompleted)

    def onStageSubmitted(self, stageSubmitted):
        self.on_spark_event('stageSubmitted', stageSubmitted)

    def onTaskEnd(self, taskEnd):
        self.on_spark_event('taskEnd', taskEnd)

    def onTaskGettingResult(self, taskGettingResult):
        self.on_spark_event('taskGettingResult', taskGettingResult)

    def onTaskStart(self, taskStart):
        self.on_spark_event('taskStart', taskStart)

    def onUnpersistRDD(self, unpersistRDD):
        self.on_spark_event('unpersistRDD', unpersistRDD)
