import datetime
from functools import cached_property
from typing import Any

from log import logger
from pydantic import (
    AliasChoices,
    AwareDatetime,
    BaseModel,
    Field,
    computed_field,
    field_serializer,
    model_validator,
    field_validator,
)

from .lpsn_interface import lpsn_id, name_parts


class BaseReference(BaseModel):
    authors: str
    title: str
    journal: str
    volume: str
    pages: str
    year: int
    pubmed_id: str
    path: str


class Triple(BaseModel, frozen=True):
    subject: int
    object: int


class HasEnzyme(Triple):
    pass


class HasSpecies(Triple):
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
    enzymes: dict[int, set[str]] = Field(
        description="Dictionary indexed by EC numbers, each of which corresponds to an EC class linked to the document in the BRENDA database. The values of the dictionary are the synonyms of the corresponding EC class registered in BRENDA.",
        default={},
    )
    bacteria: dict[int, set[str]] = {}
    strains: set[str | int] = set()
    other_organisms: dict[int, str] = {}
    relations: dict[str, set[Triple]] = {}

    @field_serializer("created", "modified")
    def serialize_dt(self, dt: datetime, _info):
        return dt.isoformat()


class BaseOrganism(BaseModel):
    organism: str


class Organism(BaseOrganism, frozen=True):
    id: int = Field(validation_alias=AliasChoices("id", "organism_id"))
    synonyms: frozenset[str] | None = None


class Bacteria(Organism):
    @computed_field  # type: ignore
    @cached_property
    def lpsn_id(self) -> int | None:
        return lpsn_id(self.organism)

    @field_validator("organism")
    @classmethod
    def remove_strain_designation(cls, name: str) -> str:
        return " ".join(
            term for key, term in name_parts(name).items() if key != "strain" and term
        )


class BaseEC(BaseModel):
    ec_class: str
    recommended_name: str


class EC(BaseEC, frozen=True):
    id: int = Field(alias="ec_class_id")
    synonyms: set[str] | None = None


class Culture(BaseModel, frozen=True):
    siid: int = Field(
        description="The id of the culture on StrainInfo", validation_alias="id"
    )
    strain_number: str


class Taxon(BaseModel):
    name: str
    lpsn: int | None = None
    ncbi: int | None = None


class Relation(BaseModel):
    culture: list[Culture] | None = None
    designation: list[str] | None = None


class Strain(BaseModel):
    siid: int = Field(
        description="The id of the strain on StrainInfo", validation_alias="id"
    )
    doi: str | None = None
    merged: list[int] | None = None
    bacdive: int | None = Field(description="ID of the strain on BacDive")
    taxon: Taxon | None = Field(
        description="Species to which the strain corresponds, if available"
    )
    cultures: frozenset[Culture] = Field(description="Cultures related to the strain")
    designations: frozenset[str] = Field(
        description="Designations for the strain other than the culture identifiers"
    )

    @model_validator(mode="before")
    @classmethod
    def validate_taxon(cls, data: Any) -> Any:
        if isinstance(data, dict) and "taxon" not in data:
            logger().warning("StrainInfo has no taxon information for %d" % data["id"])
            data["taxon"] = None

        return data


class Store(BaseModel):
    documents: dict[int, Document] = {}
    enzymes: dict[int, EC] = {}
    bacteria: dict[int, Bacteria] = {}
    strains: dict[int, Strain] = {}
