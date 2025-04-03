"""Script to compute useful statistics about the dataset"""

import math
from collections import Counter

from brenda_references.config import config
from tinydb import TinyDB, where
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage

import matplotlib.pyplot as plt


def main() -> None:
    with TinyDB(config["documents"], storage=CachingMiddleware(JSONStorage)) as docdb:
        documents = docdb.table("documents")
        print("Number of references:", len(documents))

        references_without_abstract = documents.search(
            (~(where("abstract").exists()) | (where("abstract") == ""))
        )
        print(
            "Number of references without an abstract:",
            len(references_without_abstract),
        )

        bacdocs = documents.search(
            (where("bacteria").exists()) & ~(where("bacteria") == {}),
        )
        print("Number of references mentioning bacteria:", len(bacdocs))

        print("Number of bacterial species:", len(docdb.table("bacteria")))
        print("Number of bacterial strains:", len(docdb.table("strains")))

        strain_docs_count = len(
            documents.search(
                (where("strains").exists()) & ~(where("strains") == []),
            )
        )

        print(
            f"Number of references resolved at the strain level: "
            f" {strain_docs_count} ({strain_docs_count / len(bacdocs):.2%})"
        )

        pmc_open = documents.search(where("pmc_open") == True)
        print("Number of open access references:", len(pmc_open))

        pmc_open_to_be_resolved = documents.search(
            (where("pmc_open") == True)
            & (where("bacteria").exists())
            & ~(where("bacteria") == {})
            & (~(where("strains").exists()) | ~(where("strains") == []))
        )
        print(
            "Open access references to be resolved at the strain level:",
            len(pmc_open_to_be_resolved),
        )

        has_enzyme = Counter(
            (rel["subject"], rel["object"])
            for doc in documents
            for rel in doc["relations"].get("HasEnzyme", [])
            if rel["subject"] in doc["strains"]
        )
        print("Number of enzyme-strain relation instances:", has_enzyme.total())
        print("Number of unique enzyme-strain relations:", len(has_enzyme))

        print("Most common enzyme-strain relations:", has_enzyme.most_common(5))

        nhapaxes = len([val for val in has_enzyme.values() if val == 1])
        print(
            f"Number of hapax enzyme-strain relations: {nhapaxes} "
            f"({nhapaxes / len(has_enzyme):.2%})"
        )

        related_strains = Counter(rel[0] for rel in has_enzyme.keys())
        related_enzymes = Counter(rel[1] for rel in has_enzyme.keys())

        top_strains = related_strains.most_common(
            math.ceil(len(related_strains) * 0.01)
        )
        enzyme_ratio = 0.03
        top_enzymes = related_enzymes.most_common(
            math.ceil(len(related_enzymes) * enzyme_ratio)
        )

        print(
            f"The 1% ({len(top_strains)}) most commonly related strains account "
            f"for {sum(c[1] for c in top_strains) / related_strains.total():.2%} "
            "of all relations."
        )

        print(
            f"The {enzyme_ratio:.2%} ({int(len(related_enzymes) * enzyme_ratio)})"
            " most commonly related strains account "
            f"for {sum(c[1] for c in top_enzymes) / related_enzymes.total():.2%}"
            " of all relations."
        )

        # fig, ax = plt.subplots(nrows=1, ncols=1)  # create figure & 1 axis
        # ax.hist(related_strains.values())
        # fig.savefig("plot.png")  # save the figure to file
        # plt.close(fig)  # close the figure window
