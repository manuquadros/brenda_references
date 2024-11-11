import tomllib
from pathlib import Path

with open("config.toml", mode="rb") as cf:
    config = tomllib.load(cf)

config["documents"] = (
    Path(__file__).absolute().parent.parent.parent / config["documents"]
)
