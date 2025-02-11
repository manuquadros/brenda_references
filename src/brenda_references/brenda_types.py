import datetime
from collections.abc import Mapping
from enum import StrEnum
from functools import cached_property
from typing import Annotated, Any, NamedTuple, Self, TypeAlias

from pydantic import (
    AliasChoices,
    AwareDatetime,
    BaseModel,
    Field,
    computed_field,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic.functional_serializers import PlainSerializer

from log import logger

from .lpsn_interface import lpsn_id, name_parts


def serialize_in_order(items: set[str | int]) -> list[str | int]:
    return sorted(items)


def serialize_mapping_in_order(mapping: Mapping[Any, set[Any]]) -> dict[Any, list[Any]]:
    return {key: serialize_in_order(items) for key, items in mapping.items()}


StringSet: TypeAlias = Annotated[
    frozenset[str],
    Field(default=frozenset()),
    PlainSerializer(serialize_in_order),
]

IntSet: TypeAlias = Annotated[
    frozenset[int],
    Field(default=frozenset()),
    PlainSerializer(serialize_in_order),
]


class RDFClass(StrEnum):
    D3OBacteria = "d3o:Bacteria"
    D3OEnzyme = "d3o:Enzyme"
    D3OStrain = "d3o:Strain"


class Triple(BaseModel, frozen=True):  # type: ignore
    subject: int
    object: int

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, Triple):
            return NotImplemented

        return self.subject == other.subject and self.object == other.object

    def __ne__(self, other: Self) -> bool:
        return not self == other

    def __lt__(self, other: Self) -> bool:
        if self.subject == other.subject:
            return self.object < other.object
        return self.subject < other.subject

    def __le__(self, other: Self) -> bool:
        return self < other or self == other

    def __gt__(self, other: Self) -> bool:
        return self != other and (not self < other)

    def __ge__(self, other: Self) -> bool:
        return not self < other


StringToTripleSetMapping: TypeAlias = Annotated[
    dict[str, set[Triple]],
    Field(default={}),
    PlainSerializer(serialize_mapping_in_order),
]


class BaseReference(BaseModel):
    authors: str
    title: str
    journal: str
    volume: str
    pages: str
    year: int
    pubmed_id: str
    path: str


class HasEnzyme(Triple):
    pass


class HasSpecies(Triple):
    pass


class EntityMarkup(BaseModel):
    model_config = {"frozen": True}

    start: int
    end: int
    entity_id: int
    label: str

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, EntityMarkup):
            return NotImplemented

        return all(
            getattr(self, field) == getattr(other, field)
            for field in self.model_fields_set
        )

    def __ne__(self, other: Self) -> bool:
        return not self == other

    def __lt__(self, other: Self) -> bool:
        for field in ("start", "end", "label", "entity_id"):
            attr1, attr2 = getattr(self, field), getattr(other, field)

            if attr1 < attr2:
                return True
            if attr1 > attr2:
                return False

        return False

    def __le__(self, other: Self) -> bool:
        return self < other or self == other

    def __gt__(self, other: Self) -> bool:
        return self != other and (not self < other)

    def __ge__(self, other: Self) -> bool:
        return not self < other


EntityMarkupSet: TypeAlias = Annotated[
    frozenset[EntityMarkup],
    Field(default=frozenset()),
    PlainSerializer(serialize_in_order),
]


class Document(BaseReference):
    def model_post_init(self, __context: Any) -> None:
        self.reviewed = self.created

    pmc_id: str | None = None
    pmc_open: bool | None = None
    doi: str | None = None
    created: AwareDatetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC),
        frozen=True,
    )
    reviewed: AwareDatetime | None = Field(
        description=(
            "Last time the document the information in the document"
            " was checked. In particular, when abstract retrieval and entity"
            " span annotation was last attempted."
        ),
        default=None,
    )
    abstract: str | None = None
    enzymes: IntSet = Field(
        description="Set of BRENDA IDs for each EC Class linked to this reference.",
        default={},
    )
    bacteria: dict[int, str] = {}
    strains: IntSet
    other_organisms: dict[int, str] = {}
    relations: StringToTripleSetMapping
    entity_spans: EntityMarkupSet

    @field_serializer("created", "reviewed")
    def serialize_dt(self, dt: datetime.datetime, _info):
        return dt.isoformat()


class BaseOrganism(BaseModel):
    organism: str


class Organism(BaseOrganism, frozen=True):  # type: ignore
    id: int = Field(validation_alias=AliasChoices("id", "organism_id"))
    synonyms: StringSet


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
    synonyms: StringSet


class Culture(BaseModel, frozen=True):
    siid: int = Field(
        description="The id of the culture on StrainInfo",
        validation_alias="id",
    )
    strain_number: str


class Taxon(BaseModel):
    name: str
    lpsn: int | None = None
    ncbi: int | None = None


class Relation(BaseModel):
    culture: list[Culture] | None = None
    designation: list[str] | None = None


class StrainRef(NamedTuple):
    id: int
    name: str


class Strain(BaseModel):
    id: int | None = Field(
        description="The id of the strain on StrainInfo, if found.",
        default=None,
    )
    doi: str | None = None
    merged: list[int] | None = None
    bacdive: int | None = Field(description="ID of the strain on BacDive", default=None)
    taxon: Taxon | None = Field(
        description="Species to which the strain corresponds, if available",
        default=None,
    )
    cultures: Annotated[
        frozenset[Culture],
        Field(description="Cultures related to the strain", default=frozenset()),
        PlainSerializer(lambda cultures: list(cultures)),
    ]
    designations: Annotated[
        StringSet,
        Field(
            description="Designations for the strain other than the culture identifiers",
        ),
    ]

    @model_validator(mode="before")
    @classmethod
    def validate_taxon(cls, data: Any) -> Any:
        if isinstance(data, dict) and "taxon" not in data:
            logger().warning(f"StrainInfo has no taxon information for {data}")
            data["taxon"] = None

        return data


class Store(BaseModel):
    documents: dict[int, Document] = {}
    enzymes: dict[int, EC] = {}
    bacteria: dict[int, Bacteria] = {}
    strains: dict[int, Strain] = {}
