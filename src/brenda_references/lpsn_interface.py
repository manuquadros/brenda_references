from .config import config
import pandas as pd
from cacheout import Cache
from typing import cast
from log import logger
from functools import lru_cache

cache = Cache()


@cache.memoize()
def get_lpsn() -> pd.DataFrame:
    df = pd.read_csv(config["sources"]["lpsn"])
    df = df.drop(
        ["reference", "authors", "risk_grp", "nomenclatural_type"], axis="columns"
    )
    df = df.fillna("")

    return df


def lpsn_name(record: pd.Series) -> str:
    subs_epithet = record["subsp_epithet"]

    if subs_epithet:
        subs_epithet = "subsp. " + subs_epithet

    return " ".join((record["genus_name"], record["sp_epithet"], subs_epithet)).strip()


@lru_cache
def lpsn_synonyms(query: int | str) -> frozenset[str]:
    qtype = type(query).__name__
    match qtype:
        case "int":
            lpsn = get_lpsn()
            own_lnk = lpsn.query("record_no == @query")["record_lnk"].values[0]
            syn_records = lpsn.query("record_lnk == @query | record_no == @own_lnk")

            if syn_records.empty:
                return frozenset()

            names = syn_records.apply(lpsn_name, axis=1)
            return frozenset(names)

        case "str":
            _id = lpsn_id(cast(str, query))
            return lpsn_synonyms(_id) if _id else frozenset()

        case _:
            logger().error(
                "Invalid LPSN synonym query: %s is not int or string.", qtype
            )
            return frozenset()


@lru_cache
def lpsn_id(name: str) -> int | None:
    lpsn = get_lpsn()
    keys = {0: "genus_name", 1: "sp_epithet", 2: "subsp_epithet"}
    name_parts = (
        name.replace("subsp.", "")
        .replace("ssp.", "")
        .replace("sp.", "")
        .replace("pv.", "")
        .split()
    )
    name_parts += [""] * (3 - len(name_parts))
    query = " & ".join(
        f"{keys[key]} == @name_parts[{key}]" for key in range(len(name_parts))
    )

    try:
        record = lpsn.query(query).iloc[0]
    except IndexError:
        logger().error(
            "Couldn't find an LPSN record for %s. The query was %s, where @name_part = %s.",
            name,
            query,
            name_parts,
        )
        return None

    return int(record["record_no"])


def record_nos() -> tuple[int, ...]:
    lpsn = get_lpsn()
    nums = lpsn["record_no"]

    return tuple(nums)
