import logging
import sys

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

class ClassContextAdaptor(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['class'], msg), kwargs

class LogMeta(type):
    def __init__(cls, *args):
        super().__init__(*args)

        # Explicit name mangling
        logger_attribute_name = '_' + cls.__name__ + '__logger'

        # Logger name derived accounting for inheritance for the bonus marks
        logger_name = '.'.join([c.__name__ for c in cls.mro()[-2::-1]])

        logger = logging.getLogger(logger_name)
        adapter = ClassContextAdaptor(logger, {'class': cls.__name__})

        setattr(cls, logger_attribute_name, logging.getLogger(logger_name))
