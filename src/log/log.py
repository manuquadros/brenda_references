import logging
from logging import handlers
from pathlib import Path
from cacheout import Cache

cache = Cache()

logfile = (
    Path(__file__).absolute().parent.parent.parent / "brenda_references.log"
).as_posix()


@cache.memoize()
def logger(level=logging.DEBUG, filename=logfile):
    logging.basicConfig(
        encoding="utf-8",
    )
    handler = handlers.RotatingFileHandler(
        filename=filename, maxBytes=512000, backupCount=5
    )
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s, %(module)s.%(funcName)s, %(levelname)s, %(message)s",
            datefmt="%d %b %Y %H:%M:%S",
        )
    )
    _logger = logging.getLogger(__name__)
    _logger.setLevel(level)
    _logger.addHandler(handler)

    return _logger
