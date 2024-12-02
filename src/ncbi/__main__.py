import pprint
import time
from typing import Any, Self
from urllib3 import Retry
import os

import requests
import xmltodict
from log import logger
from utils import maybe_wait, retry_if_too_many_requests


class NCBIAdapter:
    def __init__(self) -> None:
        try:
            self.api_key = os.environ["NCBI_API_KEY"]
        except KeyError:
            print(
                "Continuing without API key. If you want to go faster, set the ",
                "NCBI_API_KEY environment variable.",
            )

    def __enter__(self) -> Self:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            max_retries=Retry(connect=4, backoff_factor=0.5)
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({"Accept-Encoding": "gzip, deflate"})

        self.session = session

        return self

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        self.session.close()

    def summary_url(self, pubmed_id: str) -> str:
        url = (
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?"
            f"db=pubmed&id={pubmed_id}"
        )

        if hasattr(self, "api_key"):
            url += f"&api_key={self.api_key}"

        return url

    @staticmethod
    def record_url(pmcid: str) -> str:
        return (
            "https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi?verb=GetRecord&identifier="
            f"oai:pubmedcentral.nih.gov:{pmcid}&metadataPrefix=pmc_fm"
        )

    def __request(self, url: str) -> dict[str, Any]:
        resp = self.session.get(url)
        if resp.status_code != 200:
            err = f"Request for {url} failed with status {resp.status_code}"
            logger().error(err)
            raise requests.HTTPError(err)

        return xmltodict.parse(resp.text)

    def article_ids(self, pubmed_id: str) -> dict[str, str]:
        record = self.__request(self.summary_url(pubmed_id))

        try:
            return format_esummary_fields(record)["ArticleIds"]
        except KeyError as e:
            logger().error(
                "Failed on %s. Full record:\n %s", pubmed_id, pprint.pformat(record)
            )
            raise e

    def is_pmc_open(self, pmcid: str | None) -> bool:
        if not pmcid:
            return False

        record = self.__request(self.record_url(pmcid))

        return "pmc_open" in (
            record.get("OAI-PMH", {})
            .get("GetRecord", {})
            .get("record", {})
            .get("header", {})
            .get("setSpec", [])
        )


def format_esummary_fields(fields: list[dict] | dict) -> dict[str, Any]:
    """
    Recursively merge all fields into a single dictionary.

    The function eliminates repeated '@Name' keys and '@Type' annotations.
    """

    def parse_field(field: dict) -> str | list[str] | dict:
        if "Item" not in field:
            return field.get("#text", "")

        items = field["Item"]

        if field["@Name"] in ("ArticleIds", "History"):
            return format_esummary_fields(items)

        if isinstance(items, dict):
            items = [items]

        return [item.get("#text", "") for item in items]

    if isinstance(fields, dict):
        if "@Name" in fields:
            return {fields["@Name"]: fields.get("#text", "")}

        try:
            return format_esummary_fields(fields["eSummaryResult"]["DocSum"]["Item"])
        except KeyError as e:
            # This may happen when BRENDA has the wrong Pubmed ID for an item.
            logger().error(
                "Invalid ESummary structure: missing %s. Error:\n %s",
                e,
                fields["eSummaryResult"]["ERROR"],
            )
            return {}

    return {field["@Name"]: parse_field(field) for field in fields}
