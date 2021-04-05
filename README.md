pyspark-spy
===========

![pyspark version](https://img.shields.io/badge/pyspark-2.3%2C%202.4%2C%203.0-success)
![python version](https://img.shields.io/badge/python-3.5%2C%203.6%2C%203.7-informational)
[![Build Status](https://travis-ci.org/sashgorokhov/pyspark-spy.svg?branch=master)](https://travis-ci.org/sashgorokhov/pyspark-spy)

Collect and aggregate on spark events for profitz. In 🐍 way!

## Installation

```shell
pip install pyspark-spy
```

## How to
You register a listener
```python
import pyspark_spy
listener = pyspark_spy.PersistingSparkListener()
pyspark_spy.register_listener(spark_context, listener)
```

Execute your spark job as usual
```python
spark_context.range(1, 100).count()
```

And you have all metrics collected!

```python
print(listener.stage_output_metrics_aggregate())
OutputMetrics(bytesWritten=12861, recordsWritten=2426)
```

Look Ma, no actions!

Tested on python 3.5 - 3.7 and pyspark 2.3 - 3.0

## Available listeners

- `pyspark_spy.interface.SparkListener` - Base listener class. 
  It defines `on_spark_event(event_name, java_event)` method that you can implement yourself 
  for custom logic when any event is received.
  
- `LoggingSparkListener` - just logs event names received into supplied or automatically created logger.
- `StdoutSparkListener` - writes event names into stdout
- `PersistingSparkListener` - saves spark events into internal buffer
- `ContextSparkListener` - same as PersistingSparkListener but also allows you to record only events 
  occured within python context manager scope. More on that later

### PersistingSparkListener

Spark events collected (as java objects):
- applicationEnd
- applicationStart
- blockManagerRemoved
- blockUpdated
- environmentUpdate
- executorAdded
- executorMetricsUpdate
- executorRemoved
- jobEnd
- jobStart
- otherEvent
- stageCompleted
- stageSubmitted
- taskEnd
- taskGettingResult
- taskStart
- unpersistRDD

```python
listener.java_events['executorMetricsUpdate'] # -> List of py4j java objects
```

> View all possible spark events and their fields https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/scheduler/SparkListener.html

Events converted to python objects:
- jobEnd
- stageCompleted

```python
listener.python_events['jobEnd']  # -> List of java events converted to typed namedtuples.
listener.jobEnd  # same
```

### Available aggregations
Only in `PersistingSparkListener` and `ContextSparkListener`

- `stage_input_metrics_aggregate` - sums up all `stageCompleted` event inputMetrics into one
```python
print(listener.stage_input_metrics_aggregate())
InputMetrics(bytesRead=21574, recordsRead=584)
```
- `stage_output_metrics_aggregate` - sums up all `stageCompleted` event outputMetrics into one
```python
print(listener.stage_output_metrics_aggregate())
OutputMetrics(bytesWritten=12861, recordsWritten=2426)
```

### ContextSparkListener

To collect events from different actions and to build separate aggregations, use `ContextSparkListener`.
```python
listener = ContextSparkListener()
register_listener(sc, listener)

with listener as events: # events is basically another listener
    run_spark_job()
events.stage_output_metrics_aggregate()  # events collected only within context manager

with listener as events_2:
    run_other_spark_job()
events_2.stage_output_metrics_aggregate()  # metrics collected during second job

listener.stage_output_metrics_aggregate() # metrics collected for all jobs
```