from airflow.models import Variable

import logging

LOGGING_CLIENT = None

def get_logging_client():
    global LOGGING_CLIENT

    if LOGGING_CLIENT is None:
        logging.info('Initialising logging_client')

        log_level = Variable.get('CF_logging_level', 'INFO')

        numeric_level = getattr(logging, log_level.upper())

        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % log_level)
        logging.basicConfig()
        logging_client = logging.getLogger()
        logging_client.setLevel(numeric_level)
        LOGGING_CLIENT = logging_client

    return LOGGING_CLIENT
