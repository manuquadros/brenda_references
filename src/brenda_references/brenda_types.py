from pydantic import BaseModel, PositiveInt, AwareDatetime, Field, computed_field
from functools import cached_property
import datetime
from typing import Any
from .lpsn_interface import lpsn_id
from log import logger
from pprint import pformat


class BaseReference(BaseModel):
    authors: str
    title: str
    journal: str
    volume: str
    pages: str
    pubmed_id: str
    path: str


type EntityNames = dict[int, frozenset[str]]


class Document(BaseReference):
    def model_post_init(self, __context: Any) -> None:
        self.modified = self.created

    pmc_id: str | None = None
    pmc_open: bool | None = None
    doi: str | None = None
    created: AwareDatetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC), frozen=True
    )
    modified: AwareDatetime | None = None
    enzymes: EntityNames = {}
    bacteria: EntityNames = {}
    strains: EntityNames = {}


class BaseOrganism(BaseModel):
    organism_id: PositiveInt
    organism: str


class Organism(BaseOrganism):
    synonyms: frozenset[str] | None = None


class Bacteria(Organism):
    @computed_field  # type: ignore
    @cached_property
    def lpsn_id(self) -> int | None:
        return lpsn_id(self.organism)


class Strain(Organism):
    straininfo_id: int | None = None
    taxon: str
    cultures: frozenset[str] | None


class BaseEC(BaseModel):
    ec_class_id: PositiveInt
    ec_class: str
    recommended_name: str


class EC(BaseEC):
    synonyms: frozenset[str] | None = None
