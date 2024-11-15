import logging
from logging import handlers
from pathlib import Path

logfile = (
    Path(__file__).absolute().parent.parent.parent / "brenda_references.log"
).as_posix()


def get_logger(level=logging.WARNING, filename=logfile):
    handler = handlers.RotatingFileHandler(
        filename=filename, maxBytes=512000, backupCount=5
    )
    logging.basicConfig(
        encoding="utf-8",
        level=level,
        handlers=(handler,),
        format=f"%(asctime)s, %(module)s.%(funcName)s, %(levelname)s, %(message)s",
        datefmt="%d %b %Y %H:%M:%S",
    )

    return logging.getLogger("brenda-references")
