"""
Sometimes it is necessary to provide a file-like object when you really want to log. LoggerWriter will go in disguise.

Simple class with write() and flush() methods available, but translates the messages to a logger.
"""


class LoggerWriter:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message != '\n':
            self.logger.log(self.level, message)

    def flush(self):
        return
