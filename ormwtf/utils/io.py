import yaml

# ----------------------- #


def read_yaml(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ....................... #


def read_text(path: str):
    with open(path, "r") as f:
        return f.read()
