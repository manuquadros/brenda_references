from sqlalchemy import URL
from sqlalchemy.engine import TupleResult
from sqlmodel import Session, create_engine, SQLModel, Field, select
import tomllib


class Protein_Connect(SQLModel, table=True):  # type: ignore
    protein_connect_id: int = Field(primary_key=True)
    organism_id: int = Field(nullable=False)
    ec_class_id: int = Field(nullable=False)
    protein_organism_strain_id: int | None = Field()
    reference_id: int = Field(nullable=False)
    protein_id: int = Field(nullable=False)


class Reference(SQLModel, table=True):  # type: ignore
    reference_id: int = Field(primary_key=True)
    authors: str = Field(nullable=False)
    title: str = Field(nullable=False)
    journal: str = Field()
    volume: str = Field()
    pages: str = Field()
    year: int = Field()
    pubmed_id: str = Field()
    path: str = Field()


class Organism(SQLModel, table=True):  # type: ignore
    organism_id: int = Field(primary_key=True)
    organism: str = Field(nullable=False)


class EC_Class(SQLModel, table=True):  # type: ignore
    ec_class_id: int = Field(primary_key=True)
    ec_class: str = Field(nullable=False)
    recommended_name: str = Field(nullable=False)


class Protein(SQLModel, table=True):  # type: ignore
    protein_id: int = Field(primary_key=True)
    protein: str | None = Field()


def protein_connect_records(user: str, password: str) -> TupleResult:
    with open("config.toml", mode="rb") as cf:
        db_conn_info = tomllib.load(cf)

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
            .where(Protein_Connect.protein_organism_strain_id is None)
        )

        return session.exec(query)
