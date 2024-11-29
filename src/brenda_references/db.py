import os
from typing import Iterable, Any

from rapidfuzz import fuzz, process
from sqlalchemy import URL, Engine
from sqlalchemy.engine import Row, TupleResult
from sqlmodel import Field, Session, SQLModel, create_engine, select
from functools import lru_cache

from .brenda_types import (
    EC,
    Bacteria,
    BaseEC,
    BaseOrganism,
    BaseReference,
    Document,
    HasEnzyme,
    HasSpecies,
    Organism,
    RelationTriple,
)
from .config import config
from .straininfo import get_strain_ids


class Protein_Connect(SQLModel, table=True):  # type: ignore
    __table_args__ = {"keep_existing": True}
    __tablename__ = "protein_connect"
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


def brenda_references(engine: Engine) -> list[_Reference]:
    with Session(engine) as session:
        query = select(_Reference).limit(100)
        return session.exec(query).fetchall()


def clean_name(model: SQLModel, name_field: str) -> tuple[SQLModel, bool]:
    _, cleaned, name = getattr(model, name_field).rpartition("no activity in ")
    return model.copy(update={name_field: name}), bool(cleaned)


def brenda_enzyme_relations(engine: Engine, reference_id: int) -> dict[str, set[Any]]:
    """
    Return relation triples and their participating entities for `reference_id`
    """
    with Session(engine) as session:
        query = (
            select(Protein_Connect, _Organism, _EC, _Strain)
            .join(_Organism, Protein_Connect.organism_id == _Organism.organism_id)
            .join(_EC, Protein_Connect.ec_class_id == _EC.ec_class_id)
            .outerjoin(
                _Strain, Protein_Connect.protein_organism_strain_id == _Strain.id
            )
            .where(Protein_Connect.reference_id == reference_id)
        )
        records = session.exec(query).fetchall()

    output = {
        key: set()
        for key in ("triples", "enzymes", "bacteria", "strains", "other_organisms")
    }

    for record in records:
        organism, no_activity_organism = clean_name(record._Organism, "organism")

        if record._Strain:
            strain, no_activity_strain = clean_name(record._Strain, "name")

            if not no_activity_strain:
                output["triples"].add(
                    HasEnzyme(subject=strain.id, object=record._EC.ec_class_id),
                )

            output["triples"].add(
                HasSpecies(subject=strain.id, object=organism.organism_id)
            )
            output["strains"].add(strain)
        else:
            if not no_activity_organism:
                output["triples"].add(
                    HasEnzyme(
                        subject=organism.organism_id,
                        object=record._EC.ec_class_id,
                    )
                )

        if is_bacteria(organism.organism):
            output["bacteria"].add(
                Bacteria.model_validate(organism, from_attributes=True)
            )
        else:
            output["other_organisms"].add(
                Organism.model_validate(organism, from_attributes=True)
            )

        output["enzymes"].add(EC.model_validate(record._EC, from_attributes=True))

    return output


@lru_cache(maxsize=512)
def ec_synonyms(engine: Engine, ec_class_id: int) -> list[tuple[str, int]]:
    """
    For a given EC class, fetch a synonym, reference_id pairs.
    """
    with Session(engine) as session:
        query = (
            select(EC_Synonyms.synonyms, EC_Synonyms_Connect.reference_id)
            .join_from(
                EC_Synonyms,
                EC_Synonyms_Connect,
                EC_Synonyms_Connect.synonyms_id == EC_Synonyms.synonyms_id,
            )
            .where(EC_Synonyms_Connect.ec_class_id == ec_class_id)
        )

        synonyms = session.exec(query).all()

    return synonyms
