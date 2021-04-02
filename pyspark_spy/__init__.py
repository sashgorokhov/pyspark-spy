import logging
from contextlib import contextmanager

from pyspark import SparkContext

from pyspark_spy.spark_events import RememberingSparkListener, SparkEvents

logger = logging.getLogger(__name__)


@contextmanager
def capture_spark_events(sc: SparkContext, listener_class=RememberingSparkListener, events_class=SparkEvents):
    callback_server_started = sc._gateway.start_callback_server()
    listener = listener_class()
    events = events_class()
    sc._jsc.sc().addSparkListener(listener)
    try:
        yield events
    finally:
        try:
            events.parse_listener_events(listener=listener)
        except:
            logger.exception('Error parsing listener "%s" events', listener_class.__name__)

        sc._jsc.sc().removeSparkListener(listener)
        listener.stop_recording = True

        if callback_server_started:
            sc._gateway.close(keep_callback_server=False, close_callback_server_connections=True)
