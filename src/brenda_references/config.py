import tomllib

with open("config.toml", mode="rb") as cf:
    config = tomllib.load(cf)
