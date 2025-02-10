import tomllib

from rapidfuzz import fuzz, process
from sqlalchemy import URL
from sqlalchemy.engine import TupleResult
from sqlalchemy.sql.functions import random
from sqlmodel import Field, Session, SQLModel, create_engine, join, select

from brenda_references.types import (
    BaseReference,
    BaseOrganism,
    BaseEC,
)


class Protein_Connect(SQLModel, table=True):  # type: ignore
    __table_args__ = {"keep_existing": True}
    protein_connect_id: int = Field(primary_key=True)
    organism_id: int = Field(nullable=False)
    ec_class_id: int = Field(nullable=False)
    protein_organism_strain_id: int | None = Field()
    reference_id: int = Field(nullable=False)
    protein_id: int = Field(nullable=False)


class ReferenceTable(SQLModel, BaseReference, table=True):  # type: ignore
    __table_args__ = {"keep_existing": True}
    __tablename__ = "reference"
    reference_id: int = Field(primary_key=True)


class OrganismTable(SQLModel, BaseOrganism, table=True):  # type: ignore
    __table_args__ = {"keep_existing": True}
    __tablename__ = "organism"
    organism_id: int = Field(primary_key=True)


class ECTable(SQLModel, BaseEC, table=True):  # type: ignore
    __table_args__ = {"keep_existing": True}
    __tablename__ = "ec_class"
    ec_class_id: int = Field(primary_key=True)


class ProteinTable(SQLModel, table=True):  # type: ignore
    __table_args__ = {"keep_existing": True}
    __tablename__ = "protein"
    protein_id: int = Field(primary_key=True)


with open("config.toml", mode="rb") as cf:
    config = tomllib.load(cf)

with open(config["entities"]["bacteria"], "r") as sl:
    bacteria = set(s.strip() for s in sl.readlines())


def is_bacteria(organism: str) -> bool:
    _, ratio, _ = process.extract(organism, bacteria, scorer=fuzz.QRatio, limit=1)[0]

    return ratio > 90


def protein_connect_records(user: str, password: str) -> TupleResult:
    db_conn_info = config["database"]
    url_object = URL.create(
        drivername=db_conn_info["backend"],
        host=db_conn_info["host"],
        database=db_conn_info["database"],
        username=user,
        password=password,
    )
    engine = create_engine(url_object, echo=True)

    with Session(engine) as session:
        query = (
            select(Protein_Connect, Organism, EC_Class, Protein, Reference)
            .join(Organism, Protein_Connect.organism_id == Organism.organism_id)
            .join(EC_Class, Protein_Connect.ec_class_id == EC_Class.ec_class_id)
            .join(Protein, Protein_Connect.protein_id == Protein.protein_id)
            .join(
                Reference,
                Protein_Connect.reference_id == Reference.reference_id,
            )
        )

        return (
            record
            for record in session.exec(query)
            if is_bacteria(record.Organism.organism)
        )
