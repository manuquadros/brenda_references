from aiotinydb import AIOTinyDB
from aiotinydb.storage import AIOJSONStorage
from tinydb.table import Document as TDBDocument
from utils import CachingMiddleware


async def preprocess(doc: TDBDocument):
    """Convert a do"""


async def run() -> None:
    async with AIOTinyDB(
        config["documents"],
        storage=CachingMiddleware(AIOJSONStorage),
    ) as docdb:
        pass
