import stackprinter

from icecream import install, ic
from .brenda_references import sync_doc_db, expand_doc, add_abstracts

__all__ = ["sync_doc_db", "expand_doc", "add_abstracts"]

ic.configureOutput(includeContext=True)
install()

stackprinter.set_excepthook(style="darkbg2")
