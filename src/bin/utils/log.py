import logging
import logging.handlers
import os
import sys
import time
import functools


local_log_dir = "c:\\logs" if os.name == 'nt' else '/tmp/logs'
LOG_DIR = os.environ.get('LOG_DIR', local_log_dir)
os.makedirs(LOG_DIR, exist_ok=True)


def _build_handler(formatter='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                   max_byte=100000000, backup_count=1, log_level=logging.DEBUG,
                   fname=None):
    assert fname is not None, 'fname should be provided'
    if isinstance(formatter, str):
        formatter = logging.Formatter(formatter)
    handler = logging.handlers.RotatingFileHandler(
        os.path.join(LOG_DIR, fname),
        maxBytes=max_byte,
        backupCount=backup_count,
        encoding='utf-8'
    )
    handler.setLevel(log_level)
    handler.setFormatter(formatter)
    return handler


class GenericLogging(object):
    def __init__(self, log_level=logging.DEBUG, stdout_log_level=logging.INFO,
                 max_bytes=1000000, backup_count=1):
        self.log_level = log_level
        self.stdout_log_level = stdout_log_level
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.today = time.strftime("%Y-%m-%d")
        self.fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        self.fmt = '[%(asctime)s] - %(levelname)s - [%(name)s.%(funcName)s:%(lineno)d] - %(message)s'

    def __call__(self, logger_name, stdout_log_level=None):
        logger = logging.getLogger(logger_name)
        logger.setLevel(self.log_level)
        formatter = logging.Formatter(self.fmt)

        # add the strean handler
        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(logging.DEBUG)
        logger.addHandler(stream_handler)

        # handy _build_handler_shortcut
        _build_handler_shortcut = functools.partial(_build_handler, formatter,
                                                    self.max_bytes, self.backup_count,
                                                    self.log_level)
        self._build_handler_shortcut = _build_handler_shortcut

        # addthe todays global handler
        gh_name = f'all.{self.today}.log'
        gh = _build_handler_shortcut(gh_name)
        logger.addHandler(gh)

        # get the handler specific to the component related to the logger_name
        extra_handlers = self._get_extra_handlers(logger_name)
        for h in extra_handlers:
            logger.addHandler(h)

        return logger

    def _get_extra_handlers(self, logger_name):
        handlers = []
        if '__main__' in logger_name:
            handler_fnames = ['module_name.log']
            handlers.extend([self._build_handler_shortcut(fname) for fname in
                             handler_fnames])
        # elif:
        #     pass
        # else:
        #     pass
        return handlers


getLogger = GenericLogging()

