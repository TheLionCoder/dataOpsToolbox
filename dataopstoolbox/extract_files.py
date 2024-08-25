# *-* encoding: utf-8 *-*
# !/usr/bin/env python3
from datetime import datetime
from pathlib import Path
from typing import List

import zipfile

import typer
from loguru import logger
from tqdm import tqdm
from typing_extensions import Annotated

from ..utils.utils import list_files

app = typer.Typer()


def extract_files(dir_path: Path, remove_unpacked_dir) -> None:
    """
    Unzip all files in a directory
    :dir_path: str: directory path
    :remove_unzipped_dr: bool: remove the unzipped directory
    """

    if not dir_path.exists():
        logger.opt(colors=True).error(f"Error: {dir_path}" f"does not exist.")
        raise FileNotFoundError(f"{dir_path} does not exist")
    try:
        logger.opt(colors=True).info(f" Extracting files from {dir_path}")
        files: List[Path] = list_files(dir_path=dir_path, file_extension="zip")
        for file_path in tqdm(
            files,
            desc="Extracting files...",
            colour="yellow",
        ):
            try:
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                unpack_dir = file_path.parent.joinpath(f"{file_path.stem}_{timestamp}")
                unpack_dir.mkdir(exist_ok=True)

                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    zip_ref.extractall(unpack_dir)
                    logger.opt(colors=True).warning(f"Extracted {file_path.name} \n")

                if remove_unpacked_dir:
                    file_path.unlink()
                    logger.opt(colors=True).warning(f"Removed {file_path.name} \n")

            except zipfile.BadZipFile:
                logger.opt(colors=True).error(
                    f"Error:{file_path} - " f"{zipfile.BadZipFile}"
                )
                bad_zip_dir = file_path.parent.joinpath(f"bad_zip_{file_path.stem}")
                bad_zip_dir.mkdir(exist_ok=True)
                file_path.rename(bad_zip_dir.joinpath(file_path.name))
                logger.opt(colors=True).warning(
                    f"Moved BadZip {file_path.name} to {bad_zip_dir} \n"
                )

            except FileNotFoundError:
                logger.opt(colors=True).error(
                    f"Error: {FileNotFoundError}-" f"{file_path.absolute()}  \n"
                )
                continue

        logger.opt(colors=True).info("Extraction complete!")

    except Exception as e:
        logger.opt(colors=True).error(f"Error: {e}")
        return None


@app.command()
def main(
    dir_path: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=False,
            dir_okay=True,
            writable=True,
            resolve_path=True,
        ),
    ],
    remove_unpacked_dir: Annotated[bool, typer.Option("--remove-unpacked-dir", "-r")],
) -> None:
    """ "
    Main function
    :dir_path: str: directory path
    :remove_unzipped_dir: bool: remove the unzipped directory
    """
    extract_files(dir_path=dir_path, remove_unpacked_dir=remove_unpacked_dir)


if __name__ == "__main__":
    app()
