import itertools
import os
import re
from pprint import pp
from typing import Any, Iterable

import httpx
from lxml import etree

from log import logger
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

    @staticmethod
    def __response_handler(url: str, response: requests.Response) -> etree._Element:
        if response.status_code != 200:
            err = f"Request for {url} failed with status {response.status_code}"
            logger().error(err)
            raise httpx.HTTPError(err)

        return etree.fromstring(response.content)

    async def request(self, url: str) -> etree._Element:
        reponse = await super().request(url)
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
        self, pubmed_ids: str | Iterable[str], batch_size=10000
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
                if abstract:
                    text = abstract.text or ""
                    abstracts[pmid] = text + "".join(
                        map(
                            lambda node: etree.tostring(node, encoding="unicode"),
                            list(abstract),
                        )
                    )

        return abstracts

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

        return "pmc-open" in record.xpath("//oai:setSpec/text()", namespaces=namespaces)
