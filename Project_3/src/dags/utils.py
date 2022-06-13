from pathlib import Path


def read_file(path: str) -> str:
    path = Path(__file__).parents[3].joinpath(path)
    with open(path) as f:
        return f.read()