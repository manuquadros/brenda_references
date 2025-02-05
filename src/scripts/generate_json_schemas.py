from brenda_references import brenda_types
from pydantic import BaseModel
from pprint import pp


def main():
    for name in dir(brenda_types):
        attr = getattr(brenda_types, name)

        if isinstance(attr, type) and issubclass(attr, BaseModel) and attr != BaseModel:
            pp(attr.model_json_schema())
