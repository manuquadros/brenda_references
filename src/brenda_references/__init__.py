import stackprinter
from icecream import ic, install

from .brenda_references import add_abstracts, expand_doc, sync_doc_db

__all__ = ["add_abstracts", "expand_doc", "sync_doc_db"]

ic.configureOutput(includeContext=True)
install()

stackprinter.set_excepthook(style="darkbg2")
