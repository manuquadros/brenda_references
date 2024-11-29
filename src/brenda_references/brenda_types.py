from pydantic import (
    BaseModel,
    PositiveInt,
    AwareDatetime,
    Field,
    computed_field,
    field_serializer,
)
from functools import cached_property
import datetime
from typing import Any, TypedDict
from .lpsn_interface import lpsn_id


class BaseReference(BaseModel):
    authors: str
    title: str
    journal: str
    volume: str
    pages: str
    pubmed_id: str
    path: str


class RelationTriple(BaseModel, frozen=True):
    subject: int
    object: int


class HasEnzyme(RelationTriple):
    pass


class HasSpecies(RelationTriple):
    pass


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
    enzymes: dict[int, set[str]] = dict()
    bacteria: dict[int, set[str]] = dict()
    strains: dict[int, set[str]] = dict()
    other_organisms: dict[int, str] = dict()
    relations: set[HasEnzyme] = set()

    @field_serializer("created", "modified")
    def serialize_dt(self, dt: datetime, _info):
        return dt.isoformat()


class BaseOrganism(BaseModel):
    organism: str


class Organism(BaseOrganism, frozen=True):
    id: int = Field(alias="organism_id")
    synonyms: frozenset[str] | None = None


class Bacteria(Organism):
    @computed_field  # type: ignore
    @cached_property
    def lpsn_id(self) -> int | None:
        return lpsn_id(self.organism)


class BaseEC(BaseModel):
    ec_class: str
    recommended_name: str


class EC(BaseEC, frozen=True):
    id: int = Field(alias="ec_class_id")
    synonyms: frozenset[str] | None = None


class Culture(BaseModel, frozen=True):
    id: int
    strain_number: str


class Taxon(BaseModel):
    name: str
    lpsn: int | None = None
    ncbi: int | None = None


class Relation(BaseModel):
    culture: list[Culture] | None = None
    designation: list[str] | None = None


class BrendaStrain(BaseModel):
    id: int = Field(alias="protein_organism_strain_id")
    name: str = Field(alias="organism_strain")


class Strain(BaseModel):
    id: int = Field(description="The strain id on StrainInfo")
    doi: str | None = None
    merged: list[int] | None = None
    bacdive: int | None = None
    taxon: Taxon
    cultures: frozenset[Culture]
    designations: frozenset[str]


class Store(BaseModel):
    documents: dict[int, Document] = dict()
    enzymes: dict[int, EC] = dict()
    bacteria: dict[int, Bacteria] = dict()
    strains: dict[int, Strain] = dict()
