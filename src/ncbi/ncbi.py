"""Module providing the NCBIAdapter class."""

import asyncio
import itertools
import os
from collections.abc import Iterable

import httpx
from log import logger
from lxml import etree
from utils import APIAdapter


class NCBIAdapter(APIAdapter):
    def __init__(self) -> None:
        super().__init__(headers={"Accept-Encoding": "gzip, deflate"})

        try:
            self.api_key = os.environ["NCBI_API_KEY"]
        except KeyError:
            print(
                "Continuing without API key. If you want to go faster, set the ",
                "NCBI_API_KEY environment variable.",
            )

        namespaces = {
            "ns": "https://dtd.nlm.nih.gov/ns/archiving/2.3/",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "mml": "http://www.w3.org/1998/Math/MathML",
            "xlink": "http://www.w3.org/1999/xlink",
        }
        for key, value in namespaces.items():
            etree.register_namespace(key, value)

    @staticmethod
    def __response_handler(
        url: str, response: httpx.Response
    ) -> etree._Element:
        if response.status_code != 200:
            err = f"Request for {url} failed with status {response.status_code}"
            logger().error(err)
            raise httpx.HTTPError(err)

        return etree.fromstring(response.content)

    async def request(self, url: str) -> etree._Element:
        response = await super().request(url)
        return self.__response_handler(url, response)

    def summary_url(self, pubmed_id: str) -> str:
        url = (
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?"
            f"db=pubmed&id={pubmed_id}"
        )

        if hasattr(self, "api_key"):
            url += f"&api_key={self.api_key}"

        return url

    async def fetch_ncbi_abstracts(
        self,
        pubmed_ids: str | Iterable[str],
        batch_size: int = 10000,
    ) -> dict[str, str]:
        """Fetch abstracts and copyright information for the given `pubmed_ids`.

        For articles that do not have an abstract available, return None.
        """
        abstracts: dict[str, str | None] = {}

        if isinstance(pubmed_ids, str):
            pubmed_ids = (pubmed_ids,)

        for batch in itertools.batched(pubmed_ids, batch_size):
            url = (
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
                f"?db=pubmed&id={','.join(batch)}&retmode=xml"
            )
            root = await self.request(url)

            for article in root.findall(".//MedlineCitation"):
                pmid = article.find("PMID").text
                abstract = article.find(".//AbstractText")

                if abstract is not None and getattr(abstract, "text", None):
                    abstracts[pmid] = abstract.text + "".join(
                        map(
                            lambda node: etree.tostring(
                                node, encoding="unicode"
                            ),
                            list(abstract),
                        ),
                    )

        return abstracts

    async def fetch_fulltext(self, pmc_id: str) -> str:
        """Fetch full text record for a single given `pmc_id`.

        :param pmc_id: PubMed Central id for full text retrieval.
        :return: serialized full text for the given `pmc_id`.
        """
        url = (
            "https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi"
            "?verb=GetRecord"
            f"&identifier=oai:pubmedcentral.nih.gov:{pmc_id}"
            "&metadataPrefix=pmc"
        )
        root = await self.request(url)
        body = root.xpath("//*[name()='body']")[0]
        return etree.tostring(body, method="c14n2").decode("utf-8")

    async def fetch_fulltext_articles(
        self,
        pmc_ids: str | Iterable[str],
    ) -> dict[str, str]:
        """Fetch full text record for the given `pmc_ids`.

        :param pmc_ids: PubMed Central ids for full text retrieval.
        :return: Dictionary mapping PMC IDs to serialized full texts.
        """
        if isinstance(pmc_ids, str):
            pmc_ids = (pmc_ids,)

        fulltext: dict[str, str] = {}

        for batch in itertools.batched(pmc_ids, n=250):
            async with asyncio.TaskGroup() as tg:
                fulltext.update(
                    {
                        _id: tg.create_task(self.fetch_fulltext(_id))
                        for _id in batch
                    },
                )

            fulltext.update(
                {
                    _id: text_task.result()
                    for _id, text_task in fulltext.items()
                },
            )

        return fulltext

    @staticmethod
    def record_url(pmcid: str) -> str:
        return (
            "https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi?verb=GetRecord&identifier="
            f"oai:pubmedcentral.nih.gov:{pmcid}&metadataPrefix=pmc_fm"
        )

    async def article_ids(self, pubmed_id: str) -> dict[str, str]:
        record = await self.request(self.summary_url(pubmed_id))

        return {
            id.attrib["Name"]: id.text
            for id in record.xpath("//Item[@Name='ArticleIds']//Item")
        }

    async def is_pmc_open(self, pmcid: str | None) -> bool:
        if not pmcid:
            return False

        record = await self.request(self.record_url(pmcid))
        namespaces = {"oai": "http://www.openarchives.org/OAI/2.0/"}

        return "pmc-open" in record.xpath(
            "//oai:setSpec/text()", namespaces=namespaces
        )
