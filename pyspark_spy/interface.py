class SparkListener(object):
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
