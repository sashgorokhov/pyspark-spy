import os
import sys

import pytest
from pyspark import SparkContext
from pyspark.sql import SparkSession

import pyspark_spy
from pyspark_spy import listeners


@pytest.fixture(scope='module')
def sc() -> SparkContext:
    os.environ['PYSPARK_PYTHON'] = sys.executable
    sc = SparkContext(
        master='local[1]',
        appName='pyspark-spy tests'
    )
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.OFF)
    yield sc
    sc.stop()
    sc._gateway.close()
    sc._gateway.shutdown()


@pytest.fixture(scope='module')
def ss(sc):
    return SparkSession.builder.getOrCreate()


@pytest.fixture(scope='module')
def listener(sc):
    l = listeners.PersistingSparkListener()
    listeners.register_listener(sc, l, listeners.StdoutSparkListener())
    return l
