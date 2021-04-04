pyspark-spy
===========

Collect and aggregate on spark events for profitz. In 🐍 way!

In a nutshell, this allows you to do something like this: 
```python
with pyspark_spy.capture_spark_events(sc) as events:
    ss.read.text('poetry.lock') \
    .select(F.explode(F.split('value', ' '))) \
    .write.parquet('/tmp/foo')

>> events.stage_input_metrics_aggregate()
InputMetrics(bytesRead=21574, recordsRead=584)
>> events.stage_output_metrics_aggregate()
OutputMetrics(bytesWritten=12861, recordsWritten=2426)
```

Look Ma, no actions!
