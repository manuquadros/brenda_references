from typing import Any
import requests
import xmltodict
from log import logger
import pprint


def get_article_ids(pubmed_id: str, api_key: str | None = None) -> dict[str, str]:
    url = (
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
        f"?db=pubmed&id={pubmed_id}"
    )

    if api_key:
        url += f"&api_key={api_key}"

    with requests.get(url, headers={"Accept-Encoding": "gzip, deflate"}) as r:
        if r.status_code != 200:
            err = (
                f"Request for PubMed ID {pubmed_id} failed with status {r.status_code}"
            )
            logger().error(err)
            raise requests.HTTPError(err)

        metadata = xmltodict.parse(r.text)

    try:
        return format_esummary_fields(metadata)["ArticleIds"]
    except KeyError as e:
        logger().error(
            f"Failed on {pubmed_id}." f" Full record:\n {pprint.pformat(metadata)}"
        )
        raise e


def is_pmc_open(pmcid: str | None) -> bool:
    """
    Given a PMC ID, get its record in PMC front matter format, and check
    whether it is available in full-text.
    """
    if not pmcid:
        return False

    url = (
        "https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi?verb=GetRecord&identifier="
        f"oai:pubmedcentral.nih.gov:{pmcid}&metadataPrefix=pmc_fm"
    )

    with requests.get(url, headers={"Accept-Encoding": "gzip, deflate"}) as r:
        if r.status_code != 200:
            err = f"Request for PMCID {pmcid} failed with status {r.status_code}"
            logger().error(err)
            raise requests.HTTPError(err)

        meta = xmltodict.parse(r.text)

    try:
        record = meta["OAI-PMH"]["GetRecord"]["record"]
    except KeyError:
        return False
    else:
        return "pmc-open" in record.get("header", {}).get("setSpec", [])


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
        else:
            try:
                return format_esummary_fields(
                    fields["eSummaryResult"]["DocSum"]["Item"]
                )
            except KeyError as e:
                # This may happen when BRENDA has the wrong Pubmed ID for an item.
                logger().error(
                    f"Invalid ESummary structure: missing {e}."
                    f" Error:\n {fields["eSummaryResult"]["ERROR"]}"
                )
                return {}

    return {field["@Name"]: parse_field(field) for field in fields}
