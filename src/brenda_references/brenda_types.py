import datetime
import string
from collections.abc import Mapping
from enum import StrEnum
from functools import cached_property
from typing import Annotated, Any, NamedTuple, Self, TypeAlias

from loggers import logger
from pydantic import (
    AliasChoices,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic.functional_serializers import PlainSerializer

from .lpsn_interface import lpsn_id, name_parts
from .pydantic_frozendict import FrozenDict


def serialize_mapping_in_order(
    mapping: Mapping[Any, set[Any]],
) -> dict[Any, list[Any]]:
    """Serialize an Any -> Set mapping by sorting each set value."""
    return {key: sorted(items) for key, items in mapping.items()}


StringSet: TypeAlias = Annotated[
    frozenset[str],
    Field(default=frozenset()),
    PlainSerializer(sorted),
]

IntSet: TypeAlias = Annotated[
    frozenset[int],
    Field(default=frozenset()),
    PlainSerializer(sorted),
]


class RDFClass(StrEnum):
    D3OBacteria = "d3o:Bacteria"
    D3OEnzyme = "d3o:Enzyme"
    D3OStrain = "d3o:Strain"


class Triple(BaseModel, frozen=True):
    subject: int
    object: int

    def __hash__(self):
        return hash((self.subject, self.object))

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
    FrozenDict[str, frozenset[Triple]],
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

    def __hash__(self):
        return hash((self.start, self.end, self.entity_id, self.label))

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
    PlainSerializer(sorted),
]


class Document(BaseReference):
    model_config = ConfigDict(frozen=True)

    pmc_id: str | None = None
    pmc_open: bool | None = None
    doi: str | None = None
    created: AwareDatetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC),
    )
    reviewed: AwareDatetime | None = Field(
        description=(
            "Last time the document the information in the document"
            " was checked. In particular, when abstract retrieval and entity"
            " span annotation was last attempted."
        ),
        default_factory=lambda: datetime.datetime.now(datetime.UTC),
    )
    abstract: str | None = None
    fulltext: str | None = None
    enzymes: IntSet = Field(
        description="Set of BRENDA IDs for each EC Class linked to this reference.",
        default={},
    )
    bacteria: FrozenDict[int, str] = {}
    strains: IntSet
    other_organisms: FrozenDict[int, str] = {}
    relations: StringToTripleSetMapping
    entity_spans: EntityMarkupSet

    @field_serializer("created", "reviewed")
    def serialize_dt(self, dt: datetime.datetime) -> str:  # noqa: PLR6301
        return dt.isoformat()

    @field_validator("pmc_id", "pubmed_id", mode="before")
    @classmethod
    def strip_invalid_chars(cls, v: str | None) -> str | None:
        if v is not None:
            translation_table = str.maketrans(
                dict.fromkeys(
                    string.whitespace
                    + string.punctuation
                    + string.ascii_letters,
                ),
            )
            return v.translate(translation_table)
        return v


class BaseOrganism(BaseModel):
    organism: str


class Organism(BaseOrganism, frozen=True):
    id: int = Field(validation_alias=AliasChoices("id", "organism_id"))
    synonyms: StringSet


class Bacteria(Organism):
    @computed_field
    @cached_property
    def lpsn_id(self) -> int | None:
        return lpsn_id(self.organism)

    @field_validator("organism")
    @classmethod
    def remove_strain_designation(cls, name: str) -> str:
        return " ".join(
            term
            for key, term in name_parts(name).items()
            if key != "strain" and term
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
    bacdive: int | None = Field(
        description="ID of the strain on BacDive", default=None
    )
    taxon: Taxon | None = Field(
        description="Species to which the strain corresponds, if available",
        default=None,
    )
    cultures: Annotated[
        frozenset[Culture],
        Field(
            description="Cultures related to the strain", default=frozenset()
        ),
        PlainSerializer(list),
    ]
    designations: Annotated[
        StringSet,
        Field(
            description="Designations other than the culture identifiers",
        ),
    ]


class Store(BaseModel):
    documents: dict[int, Document] = {}
    enzymes: dict[int, EC] = {}
    bacteria: dict[int, Bacteria] = {}
    strains: dict[int, Strain] = {}
