import logging
from logging import handlers
from pathlib import Path

logfile = (
    Path(__file__).absolute().parent.parent.parent / "brenda_references.log"
).as_posix()


def logger(
    level: int = logging.DEBUG,
    filename: str = logfile,
) -> logging.Logger:
    """Return a logger with file rotation."""
    ologger = logging.getLogger(__name__)
    ologger.setLevel(level)

    handler = handlers.RotatingFileHandler(
        filename=filename,
        maxBytes=512000,
        backupCount=5,
    )
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s, %(module)s.%(funcName)s, %(levelname)s, %(message)s",
            datefmt="%d %b %Y %H:%M:%S",
        ),
    )
    ologger.addHandler(handler)

    return ologger


def stderr_logger(level: int = logging.DEBUG) -> logging.Logger:
    """Create a simple stderr logger for debugging purposes."""
    ologger = logging.getLogger(__name__)
    ologger.setLevel(level)

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s, %(module)s.%(funcName)s, %(levelname)s, %(message)s",
            datefmt="%H:%M:%S",
        ),
    )
    ologger.addHandler(handler)

    return ologger
