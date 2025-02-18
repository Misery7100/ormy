from pathlib import Path

import yaml

# ----------------------- #


def read_yaml(path: str | Path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ....................... #


def read_text(path: str | Path):
    with open(path, "r") as f:
        return f.read()


# ....................... #


def read_template(path: str | Path, substitutions: dict[str, str]):
    with open(path, "r") as f:
        template = f.read()

    for k, v in substitutions.items():
        template = template.replace(k, v)

    return template
