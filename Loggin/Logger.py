from Configuration.ConfigurationManager.ConfigurationManager import *
import logging
import logging.config
import time
import logging.handlers


class Logger(object):
    _instance = None
    log = None

    def __init__(self):
        logging.config.dictConfig(ConfigurationManager.GetLoggerSettings())
        self.log = logging


    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
        return cls._instance

    @staticmethod
    def CreateLogger(name):
        return  Logger().log.getLogger(name)

    @staticmethod
    def LogDebug(message):
        Logger().log.debug(message)

    @staticmethod
    def LogInfo(message):
        Logger().log.info(message)

    @staticmethod
    def LogWarning(message):
        Logger().log.warning(message)

    @staticmethod
    def LogError(message):
        Logger().log.error(message)

    @staticmethod
    def LogCritical(message):
        Logger().log.critial(message)