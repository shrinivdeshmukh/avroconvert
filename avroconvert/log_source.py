# ======================================================================================================================
# This is the logger file for data coordinator service
# It returns a logger object.
# Initialization params are:
#   1. log_level: 'DEBUG','INFO','WARNING','ERROR', 'CRITICAL','LOG','EXCEPTION'
#   2. filename: output log file name. If not exists, new will be created in the logs/ folder.
# ======================================================================================================================


import logging
import os


class Logging:
    def __init__(self, log_level, filename=None):
        self.log_level = log_level
        self.root_log_dir = 'logs'
        self.filename = 'logs/{}'.format(filename)
        self.logger = logging.getLogger(__name__)

    def get_logger(self):
        logging.basicConfig(level=self.log_level,
                            format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
        return self.logger
