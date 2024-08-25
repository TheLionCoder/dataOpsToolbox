# *-* coding: utf-8 *-*
# !/usr/bin/env python3
from datetime import datetime
from pathlib import Path

import typer
from loguru import logger
from tqdm import tqdm
from typing_extensions import Annotated

from ..config import FileExtension
from ..utils.utils import compute_hash, read_file_chunks

app = typer.Typer()


def create_hash_file(dir_path: Path, file_pattern: str, hash_name: str) -> None:
    """
    Create a hash file that contains a list of files and hashes.
    Args:
        dir_path: Path to the directory where the files are located.
        file_pattern: Pattern of the files to be hashed.
        hash_name: Hash algorithm name.
    """
    date = datetime.now().strftime("%Y%m%d")
    hash_file_name: str = f"01-hashes-{dir_path.name}-{date}.txt"
    hash_file_path = dir_path.joinpath(hash_file_name)
    separator = "*-* -- *-*".center(50, "-")

    # Create a hash file that contains a list of files and hashes
    with hash_file_path.open("w") as hash_output:
        for file in dir_path.glob(f"*.{file_pattern}"):
            if file.name == hash_file_name:
                continue
            if not file.is_file():
                continue
            file_content_gen = read_file_chunks(file)
            computed_hash = compute_hash(file_content_gen, hash_name=hash_name)
            file_date = datetime.fromtimestamp(file.stat().st_mtime).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            hash_output.write(
                f"{separator}\n"
                f"file={file.name}\n"
                f"{hash_name}={computed_hash}\n"
                f"modification_date={file_date}\n"
                f"{separator}\n"
            )


@app.command()
def main(
    dir_path: Annotated[
        Path,
        typer.Option(
            exists=True, file_okay=True, dir_okay=True, writable=True, resolve_path=True
        ),
    ],
    file_pattern: Annotated[FileExtension, typer.Option(case_sensitive=False)],
    sub_folders: Annotated[bool, typer.Option("--subfolders")] = False,
    hash_name: str = "blake2b",
) -> None:
    """
    Main function.
    """
    try:
        directories = [item for item in dir_path.iterdir() if item.is_dir()]
        if sub_folders:
            for dir_path in tqdm(
                directories, desc="Hashing files...", colour="#E84855"
            ):
                create_hash_file(
                    dir_path=dir_path,
                    file_pattern=file_pattern.value,
                    hash_name=hash_name,
                )

        create_hash_file(
            dir_path=dir_path, file_pattern=file_pattern.value, hash_name=hash_name
        )
        logger.opt(colors=True).info("Files hashed successfully.")
    except Exception as e:
        logger.error(f"Error in main function:" f"{e}")


if __name__ == "__main__":
    app()
