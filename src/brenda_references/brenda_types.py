from pydantic import BaseModel, PositiveInt


class BaseReference(BaseModel):
    authors: str
    title: str
    journal: str
    volume: str
    pages: str
    pubmed_id: str
    path: str


class Document(BaseReference):
    pmc_id: str | None = None
    pmc_open: bool | None = None
    doi: str | None = None


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
