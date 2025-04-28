import tomllib
from pathlib import Path

ROOT_DIR = Path(__file__).absolute().parent.parent.parent

with open(ROOT_DIR / "config.toml", mode="rb") as cf:
    config = tomllib.load(cf)

config["documents"] = ROOT_DIR / config["documents"]

for resource in config["sources"]:
    config["sources"][resource] = ROOT_DIR / config["sources"][resource]
