import json
import os


class ConfigurationManager(object):
    _instance = None

    def __init__(self):
        with open(os.path.dirname(os.path.abspath(__file__))+'/../../appSettings.json', 'r') as f:
            self._configuration = json.load(f)

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigurationManager, cls).__new__(cls)
        return cls._instance

    @property
    def configuration(self):
        return self._configuration

    @staticmethod
    def GetConnectionString(name):
        return ConfigurationManager().configuration['ConnectionStrings'][name]

    @staticmethod
    def GetValue(key):
        return ConfigurationManager().configuration[key]

    @staticmethod
    def GetLoggerSettings():
        return ConfigurationManager().configuration['Logging']

    @staticmethod
    def TrainConfig(name):
        return ConfigurationManager().configuration['TrainConfig'][name]

    @staticmethod
    def SchedulerConfig(name):
        return ConfigurationManager().configuration['SchedulerConfig'][name]

