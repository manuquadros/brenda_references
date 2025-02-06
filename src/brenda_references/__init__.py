import stackprinter

from icecream import install, ic

ic.configureOutput(includeContext=True)
install()
from .brenda_references import sync_doc_db, expand_doc, add_abstracts

stackprinter.set_excepthook(style="darkbg2")
