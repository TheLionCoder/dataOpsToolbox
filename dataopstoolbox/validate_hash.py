# -*_ coding: utf-8 -*-
# ! /usr/bin/env python3
from pathlib import Path

import typer
from loguru import logger
from utils.utils import compute_hash, read_file_chunks
from typing_extensions import Annotated

app = typer.Typer()


def validate_hash(file_path: Path, expected_hash: str, hash_name: str) -> bool:
    """
    Validate the hash of a file against the expected hash
    :file_path: path to the file.
    :expected_hash: expected hash of the file.
    :hash_name: hashing algorithm to use.
    :return: True if the hash matches, False otherwise.
    """
    file_content = read_file_chunks(file_path)
    computed_hash = compute_hash(file_content, hash_name)
    return computed_hash == expected_hash


@app.command()
def main(
    file_path: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ],
    file_hash: str,
    hash_name: str,
) -> None:
    """
    Main function to validate the hash of a file.
    """
    if validate_hash(file_path, file_hash, hash_name=hash_name):
        logger.opt(colors=True).warning(f"Hash of {file_path} is valid.")
    else:
        logger.opt(colors=True).error(f"Hash of {file_path} is invalid.")


if __name__ == "__main__":
    app()
