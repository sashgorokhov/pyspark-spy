import os
import sys

import pytest
from pyspark import SparkContext
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
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


@pytest.fixture(scope='session')
def ss(sc):
    return SparkSession.builder.getOrCreate()
