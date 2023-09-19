import logging
from pathlib import Path
from typing import Union


def init_logger(logger_name: str = None, level: int = None, logger_format: str = None,
                tg_token: str = None, chat_id: str = None,
                log_path: Union[Path, str] = None) -> logging.Logger:
    from logging.handlers import TimedRotatingFileHandler
    import requests

    class ArgsFormatter(logging.Formatter):
        def format(self, record):
            if record.args:
                record.msg = ' '.join([str(i) for i in [record.msg, *record.args]])
                record.args = None
            return super(ArgsFormatter, self).format(record)

    # ? tested
    class PostHandler(logging.Handler):
        def __init__(self, tg_token_, chat_id_, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.tg_token = tg_token_
            self.chat_id = chat_id_
            self.url = f'https://api.telegram.org/bot{self.tg_token}/sendMessage'

        def emit(self, record):
            data = self.format(record)
            data = {'chat_id': self.chat_id, 'text': str(data)}
            requests.post(self.url, json=data, verify=False)

    logger_name = logger_name or 'rpa.robot'
    level = level or logging.INFO
    logger_format = logger_format or '%(asctime)s||%(levelname)s||%(message)s'
    tg_logger_format = '%(message)s'
    date_format = '%Y-%m-%d,%H:%M:%S'
    backup_count = 50

    logging.basicConfig(level=level, format=logger_format, datefmt=date_format)
    logger = logging.getLogger(logger_name)
    formatter = ArgsFormatter(logger_format, datefmt=date_format)
    tg_formatter = ArgsFormatter(tg_logger_format, datefmt=date_format)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)
    logger.addHandler(console_handler)

    if tg_token and chat_id:
        post_handler = PostHandler(tg_token, chat_id)
        post_handler.setFormatter(tg_formatter)
        post_handler.setLevel(level)
        logger.addHandler(post_handler)
    if log_path:
        log_path = Path(log_path).resolve()
        log_path.parent.mkdir(exist_ok=True, parents=True)
        file_handler = TimedRotatingFileHandler(log_path.__str__(), 'W3', 1, backup_count, "utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        logger.addHandler(file_handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger
