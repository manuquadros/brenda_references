from pydantic import BaseModel, PositiveInt, AwareDatetime, Field
import datetime
from typing import Any


class BaseReference(BaseModel):
    authors: str
    title: str
    journal: str
    volume: str
    pages: str
    pubmed_id: str
    path: str


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


class BaseOrganism(BaseModel):
    organism_id: PositiveInt
    organism: str


class Organism(BaseOrganism):
    synonyms: list[str] | None = None


type Bacteria = Organism
type Strain = Organism


class BaseEC(BaseModel):
    ec_class_id: PositiveInt
    ec_class: str
    recommended_name: str


class EC(BaseEC):
    synonyms: list[str] | None = None
