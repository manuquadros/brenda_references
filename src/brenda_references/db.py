import os

from rapidfuzz import fuzz, process
from sqlalchemy import URL, Engine
from sqlalchemy.engine import TupleResult
from sqlalchemy.sql.functions import random
from sqlmodel import Field, Session, SQLModel, create_engine, join, select

from brenda_references.brenda_types import BaseEC, BaseOrganism, BaseReference
from brenda_references.config import config


class Protein_Connect(SQLModel, table=True):  # type: ignore
    __table_args__ = {"keep_existing": True}
    protein_connect_id: int = Field(primary_key=True)
    organism_id: int = Field(nullable=False)
    ec_class_id: int = Field(nullable=False)
    protein_organism_strain_id: int | None = Field()
    reference_id: int = Field(nullable=False)
    protein_id: int = Field(nullable=False)


class _Reference(SQLModel, BaseReference, table=True):  # type: ignore
    __table_args__ = {"keep_existing": True}
    __tablename__ = "reference"
    reference_id: int = Field(primary_key=True)


class _Organism(SQLModel, BaseOrganism, table=True):  # type: ignore
    __table_args__ = {"keep_existing": True}
    __tablename__ = "organism"
    organism_id: int = Field(primary_key=True)


class _EC(SQLModel, BaseEC, table=True):  # type: ignore
    __table_args__ = {"keep_existing": True}
    __tablename__ = "ec_class"
    ec_class_id: int = Field(primary_key=True)


class _Protein(SQLModel, table=True):  # type: ignore
    __table_args__ = {"keep_existing": True}
    __tablename__ = "protein"
    protein_id: int = Field(primary_key=True)


class _Strain(SQLModel, table=True):  # type: ignore
    __table_args__ = {"keep_existing": True}
    __tablename__ = "protein_organism_strain"
    id: int = Field(
        sa_column_kwargs={"name": "protein_organism_strain_id"}, primary_key=True
    )
    name: str = Field(sa_column_kwargs={"name": "organism_strain"}, nullable=False)


class EC_Synonyms_Connect(SQLModel, table=True):  # type: ignore
    __table_args__ = {"keep_existing": True}
    __tablename__ = "synonyms_connect"
    synonyms_connect_id: int = Field(primary_key=True)
    ec_class_id: int
    synonyms_id: int
    reference_id: int


class EC_Synonyms(SQLModel, table=True):  # type: ignore
    __table_args__ = {"keep_existing": True}
    __tablename__ = "synonyms"
    synonyms_id: int = Field(primary_key=True)
    synonyms: str


with open(config["sources"]["bacteria"], "r") as sl:
    bacteria = set(s.strip() for s in sl.readlines())


def get_engine():
    try:
        user, password = os.environ["BRENDA_USER"], os.environ["BRENDA_PASSWORD"]
    except KeyError as err:
        err.add_note(
            "Please set the BRENDA_USER and BRENDA_PASSWORD environment variables"
        )
        raise

    db_conn_info = config["database"]
    url_object = URL.create(
        drivername=db_conn_info["backend"],
        host=db_conn_info["host"],
        database=db_conn_info["database"],
        username=user,
        password=password,
    )

    return create_engine(url_object, echo=True)


def is_bacteria(organism: str) -> bool:
    _, ratio, _ = process.extract(organism, bacteria, scorer=fuzz.QRatio, limit=1)[0]

    return ratio > 90


def protein_connect_records(engine: Engine) -> TupleResult:
    with Session(engine) as session:
        query = (
            select(Protein_Connect, _Organism, _EC, _Protein, _Reference, _Strain)
            .join(_Organism, Protein_Connect.organism_id == _Organism.organism_id)
            .join(_EC, Protein_Connect.ec_class_id == _EC.ec_class_id)
            .join(_Protein, Protein_Connect.protein_id == _Protein.protein_id)
            .join(
                _Reference,
                Protein_Connect.reference_id == _Reference.reference_id,
            )
            .join(_Strain, Protein_Connect.protein_organism_strain_id == _Strain.id)
            .limit(100)
        )
        records = session.exec(query).fetchall()

    return (record for record in records if is_bacteria(record._Organism.organism))


def ec_synonyms(
    engine: Engine, ec_class_id: int, doc_id: int | None = None
) -> set[str]:
    """
    For a given EC class, fetch a deduplicated list of its synonyms.

    If `doc_id` is provided, return only the synonyms referenced in that article.
    """
    with Session(engine) as session:
        query = (
            select(EC_Synonyms.synonyms)
            .join_from(
                EC_Synonyms,
                EC_Synonyms_Connect,
                EC_Synonyms_Connect.synonyms_id == EC_Synonyms.synonyms_id,
            )
            .where(EC_Synonyms_Connect.ec_class_id == ec_class_id)
        )

        if doc_id:
            query = query.where(EC_Synonyms_Connect.reference_id == doc_id)

        synonyms = session.exec(query).unique().all()

    return set(synonyms)
