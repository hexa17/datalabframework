import logging

try:
    from kafka import KafkaProducer
except:
    KafkaProducer=None

import socket
import getpass
import datetime
import traceback as tb
import json

import sys
import os
import git
import random

#import a few help methods
from . import project
from . import notebook
from . import params

_logger = None

def _get_session():
    # We use git hash as session id
    repo = git.Repo(search_parent_directories=True)
    clean = len(repo.index.diff(None))== 0
    if clean:
        return repo.head.object.hexsha
    else:
        return 1

def _logrecord_add_attributes(record):
    if type(record.msg) is dict and 'type' in record.msg:
        record.type = record.msg['type']
    else:
        # default type
        record.type = 'message'
    record.session = _get_session()
    return record

class TekoFormatter(logging.Formatter):
    def __init__(self,
                 fmt=None,
                 datefmt=None):
        super().__init__(fmt, datefmt)

    def format(self, record):
        logr = _logrecord_add_attributes(record)
        return super(TekoFormatter, self).format(logr)

class LogstashFormatter(logging.Formatter):
    """
    A custom formatter to prepare logs to be
    shipped out to logstash.
    """

    def __init__(self,
                 fmt=None,
                 datefmt=None):
        pass

    def format(self, record):
        """
        Format a log record to JSON, if the message is a dict
        assume an empty message and use the dict as additional
        fields.
        """

        logr =  _logrecord_add_attributes(record)
        timestamp = datetime.datetime.fromtimestamp(loginfo['created']).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        log_record = {'severity': logr.levelname,
                'session': session,
                '@timestamp': timestamp,
                'type': type,
                'fields': fields}

        return json.dumps(log_record)

class KafkaLoggingHandler(logging.Handler):

    def __init__(self, topic, bootstrap_servers):
        logging.Handler.__init__(self)

        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def emit(self, record):
        msg = self.format(record).encode("utf-8")
        self.producer.send(self.topic, msg)

    def close(self):
        if self.producer is not None:
            self.producer.flush()
            self.producer.stop()
        logging.Handler.close(self)

loggingLevels = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'fatal': logging.FATAL
}

# # TODO
# logging highlevel
# def logger.dataread(**extra):
# def logger.dataframe():

def init():
    global _logger

    md = params.metadata()

    logger = logging.getLogger()
    logger.handlers = []

    p = md['loggers'].get('kafka')
    if p and p['enable'] and KafkaProducer:

        level = loggingLevels.get(p.get('severity'))
        topic = p.get('topic')
        hosts = p.get('hosts')

        # disable logging for 'kafka.KafkaProducer'
        # to avoid infinite logging recursion on kafka
        logging.getLogger('kafka.KafkaProducer').addHandler(logging.NullHandler())

        formatterLogstash = LogstashFormatter(json.dumps({'extra':info}))
        handlerKafka = KafkaLoggingHandler(topic, hosts)
        handlerKafka.setLevel(level)
        handlerKafka.setFormatter(formatterLogstash)
        logger.addHandler(handlerKafka)


    p = md['loggers'].get('stream')
    if p and p['enable']:
        level = loggingLevels.get(p.get('severity'))
        format = p.get('log_format')

        # create console handler and set level to debug
        handler = logging.StreamHandler(sys.stdout)
        handler.formatter = TekoFormatter(format)
        # TODO : Bug with set level
        handler.setLevel(level)
        logger.addHandler(handler)

    _logger = logger

def getLogger():
    global _logger
    if not _logger:
        init()
    return _logger
