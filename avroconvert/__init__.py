"""Top-level package for avroconvert."""

__author__ = """Shrinivas Vijay Deshmukh"""
__email__ = 'shrinivas.deshmukh11@gmail.com'
__version__ = '0.1.0'

from os import getenv
from avroconvert.log_source import Logging

logger = Logging(log_level=getenv('LOG_LEVEL', 'INFO')).get_logger()

from avroconvert.avroconvert import AvroConvert
from avroconvert.sources import gs_reader, s3_reader, fs_reader
from avroconvert.execute import Execute