# *-*encoding: utf-8 *-*

from collections import defaultdict
from pathlib import Path
from typing import List, Optional

import polars as pl
import typer
from loguru import logger
from tqdm import tqdm
from typing_extensions import Annotated

from config import FileExtension

app = typer.Typer()


def _calculate_file_len(file_path: Path, has_header: bool) -> int:
    """Calculate the number of lines in a file
    :param file_path: Path to the file
    :param has_header: Whether the file has a header.
    :return: Number of lines in the file"""
    encodings: List[str] = ["utf-8", "iso-8859-1", "cp1252"]
    for encode in encodings:
        try:
            with open(file_path, "r", encoding=encode) as fin:
                if has_header:
                    next(fin)
                return sum(1 for _ in fin)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Unable to decode file {file_path}")

def _get_file_name(file: Path, slice_name_start: Optional[int],
                   slice_name_end: Optional[int]) -> str:
    """
    Get the file name
    :param file: Path to the file
    :param slice_name_start: Index to start the slice
    :param slice_name_end: Index to end the slice
    :return: file name
    """
    file_name: str = file.stem.upper()
    return file_name[slice_name_start: slice_name_end]

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
    file_extension: Annotated[FileExtension, typer.Option(case_sensitive=False)],
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
    has_header: Annotated[bool, typer.Option("--has-header")] = False,
    include_main_dir: Annotated[bool, typer.Option("--include-main-dir")] = False,
    slice_name_start: Optional[int] = None,
    slice_name_end: Optional[int] = None,
) -> None:
    """List files with a specific extension in a directory
    :param dir_path: Path to the directory
    :param file_extension: File extension
    :param has_header: Whether the file has a header
    :param verbose: Whether to print verbose output
    :param include_main_dir: Whether to include the main directory in the output
    :param slice_name_start: Start of the slice range, default is None
    :param slice_name_end: End of the slice range, default is None
    """
    fout: Path = dir_path.joinpath(f"file_count_{dir_path.stem}.xlsx")

    data = defaultdict(lambda: {"file_count": 0, "file_len": 0})
    processed_data = {
        "file": [],
        "dir": [],
        "parent": [],
        "file_count": [],
        "file_len": [],
    }
    dataframe_schema = {
        "file": pl.String,
        "dir": pl.String,
        "parent": pl.String,
        "file_count": pl.UInt8,
        "file_len": pl.UInt32,
    }

    try:
        logger.opt(colors=True).info(
            f"Listing files in {dir_path} with extension: {file_extension}."
        )
        for file in tqdm(
            dir_path.rglob(f"*.{file_extension.value}"),
            desc="Listing files",
            colour="yellow",
            dynamic_ncols=True,
        ):
            file_name = _get_file_name(file, slice_name_start, slice_name_end)
            file_dir = file.parents[1].stem
            file_parent = file.parent.stem
            file_len = _calculate_file_len(file, has_header)

            key = (file_name, file_dir, file_parent)
            data[key]["file_count"] += 1
            data[key]["file_len"] += file_len

        for (file_name, file_dir, file_parent), counts in data.items():
            processed_data["file"].append(file_name)
            processed_data["dir"].append(file_dir)
            processed_data["parent"].append(file_parent)
            processed_data["file_count"].append(counts.get("file_count", 0))
            processed_data["file_len"].append(counts.get("file_len", 0))
        raw_data = pl.DataFrame(processed_data, schema=dataframe_schema)
        grouped_df: pl.DataFrame = raw_data.group_by(["parent", "file"]).agg(
            pl.sum("file_count"), pl.sum("file_len")
        )

        df: pl.DataFrame = grouped_df if include_main_dir else raw_data

        if verbose:
            logger.opt(colors=True).info(df)

        df.write_excel(fout)

    except FileNotFoundError:
        logger.opt(colors=True).error(f"Directory: {dir_path} not found.")
    except Exception as e:
        logger.opt(colors=True).error(f"Error: {e}.")


if __name__ == "__main__":
    app()
