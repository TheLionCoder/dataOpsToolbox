# *-* encoding: utf-8 *-*
import concurrent.futures
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, List, Optional
import sys

import polars as pl
import typer
from loguru import logger
from tqdm import tqdm
from typing_extensions import Annotated

project_root: Path = Path(__file__).resolve().parents[2]
logger.opt(colors=True).info(f"Python version: {sys.version}")
sys.path.append(project_root.as_posix())
logger.opt(colors=True).debug(f"{project_root=}")

from dataopstoolbox.config import FileExtension, FileSeparator, OutputFileFormat  # noqa: E402
from dataopstoolbox.utils.utils import list_files  # noqa: E402

app = typer.Typer()


class HandleMissing(str, Enum):
    skip = "skip"
    separate = "separate"


def save_category_files(
    category: str,
    filtered_lazy_df: pl.LazyFrame,
    category_col: str,
    keep_category_col: bool,
    save_function: Callable,
    output_dir: Path,
    file_name: str,
    output_format: str,
    append_category_to_file_name: bool,
    **kwargs,
) -> None:
    category_lazy_df = filtered_lazy_df.filter(pl.col(category_col).eq(category))
    if not keep_category_col:
        category_lazy_df = category_lazy_df.select(pl.all().exclude(category_col))
    df = category_lazy_df.collect()
    save_path: Path = (
        output_dir.joinpath(category, f"{file_name}.{output_format}")
        if not append_category_to_file_name
        else output_dir.joinpath(f"{file_name}{category}.{output_format}")
    )
    save_function(df, save_path, **kwargs)


def split_and_save_by_category(
    lazy_df: pl.LazyFrame,
    *,
    categories: List[str],
    file_name: str,
    category_col: str,
    output_dir: Path,
    keep_category_col: bool,
    output_format: str,
    append_category_to_file_name: bool,
    **kwargs,
) -> None:
    """
    Split the datarame by category and save the files in the output directory.
    :param lazy_df: LazyFrame to split
    :param categories: List of categories to split in the dataframe
    :param file_name: Name of the file to save
    :param category_col: Column to split the dataframe by
    :param output_dir: Output directory to save the files
    :param keep_category_col: Keep the category column in the output files
    :param output_format: Output file format
    :param append_category_to_file_name: Append category to the file name
    """
    save_functions: Dict[str, Callable] = {
        OutputFileFormat.csv: pl.DataFrame.write_csv,
        OutputFileFormat.txt: pl.DataFrame.write_csv,
        OutputFileFormat.parquet: pl.DataFrame.write_parquet,
        OutputFileFormat.xlsx: pl.DataFrame.write_excel,
    }
    save_function: Callable = save_functions.get(output_format, pl.DataFrame.write_csv)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                save_category_files,
                category,
                lazy_df,
                category_col,
                keep_category_col,
                save_function,
                output_dir,
                file_name,
                output_format,
                append_category_to_file_name,
                **kwargs,
            )
            for category in categories
        ]
        for future in tqdm(
            concurrent.futures.as_completed(futures),
            desc="Saving files",
            colour="yellow",
        ):
            future.result()


@pl.StringCache()
def _extract_unique_categories(
    lazy_df: pl.LazyFrame, *, category_col: str
) -> List[Optional[str]]:
    """
    Extract unique categories from the dataframe.
    :param lazy_df: LazyFrame to extract unique categories from
    :param category_col: Column to extract unique categories from
    :return: List of unique categories
    """
    return (
        lazy_df.select(pl.col(category_col).cast(pl.Categorical))
        .unique()
        .collect()
        .get_column(category_col)
        .to_list()
    )


def process_file(
    file_path: Path,
    output_dir: Path,
    extension: str,
    separator: str,
    category_col: str,
    verbose: bool,
    make_dir: bool,
    keep_category_col: bool,
    output_format: str,
    output_separator: str,
    fill_null_value: Optional[str] = None,
) -> None:
    """
    Process files in the input directory and split them by category
    and save them in the output directory.
    :param file_path: Path to the input directory
    :param output_dir: Path to the output directory
    :param extension: File extension of the files to process
    :param separator: Separator for csv files
    :param category_col: Column to split the files by
    :param verbose: Print verbose output
    :param make_dir: Make a directory for each category
    :param keep_category_col: Keep the category column in the output files
    :param output_format: Output file format
    :param output_separator: Separator for output files
    :param fill_null_value: Value to fill missing categories with,
    ignored if handle missing skipping
    """
    read_functions: Dict[str, Callable] = {
        FileExtension.csv: pl.scan_csv,
        FileExtension.txt: pl.scan_csv,
        FileExtension.parquet: pl.scan_parquet,
    }

    file_name: str = file_path.stem
    read_function: Callable = read_functions.get(extension, pl.scan_csv)

    if extension in ["csv", "txt"]:
        lazy_df: pl.LazyFrame = read_function(
            file_path, separator=separator, infer_schema=False
        )
    else:
        lazy_df: pl.DataFrame = read_function(file_path)

    if fill_null_value is not None:
        query: pl.LazyFrame = lazy_df.with_columns(
            pl.col(category_col).fill_null(fill_null_value)
        )
    else:
        query: pl.LazyFrame = lazy_df.filter(pl.col(category_col).is_not_null())

    if category_col not in query.collect_schema().names():
        logger.opt(colors=True).warning(
            f"Column {category_col} not found in {file_name}" f" skipping the file...."
        )
        return

    categories: List[str] = _extract_unique_categories(
        query, category_col=category_col, fill_null_value=fill_null_value
    )
    if verbose:
        logger.opt(colors=True).info(f"Splitting  {file_name}" f" in {categories}...")

    if make_dir:
        for category in categories:
            fout_dir = output_dir.joinpath(category)
            fout_dir.mkdir(parents=True, exist_ok=True)

    if output_format in ["csv", "txt"]:
        split_and_save_by_category(
            query,
            separator=output_separator,
            categories=categories,
            file_name=file_name,
            category_col=category_col,
            output_dir=output_dir,
            keep_category_col=keep_category_col,
            output_format=output_format,
            append_category_to_file_name=not make_dir,
        )
    else:
        split_and_save_by_category(
            query,
            categories=categories,
            file_name=file_name,
            category_col=category_col,
            output_dir=output_dir,
            keep_category_col=keep_category_col,
            output_format=output_format,
            append_category_to_file_name=not make_dir,
        )


@app.command()
def main(
    category_col: str,
    input_path: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=True,
            dir_okay=True,
            writable=False,
            resolve_path=True,
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=False,
            dir_okay=True,
            writable=True,
            resolve_path=True,
        ),
    ],
    extension: Annotated[FileExtension, typer.Option(case_sensitive=False)],
    output_format: Annotated[OutputFileFormat, typer.Option(case_sensitive=False)],
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
    keep_category_col: Annotated[
        bool, typer.Option("--keep-category-col", "-k")
    ] = False,
    make_dir: Annotated[bool, typer.Option("--make-dir")] = False,
    separator: Annotated[
        FileSeparator,
        typer.Option(case_sensitive=False, help="Separator for input files"),
    ] = FileSeparator.comma,
    output_separator: Annotated[
        FileSeparator,
        typer.Option(case_sensitive=False, help="Separator for output files"),
    ] = FileSeparator.comma,
    fill_null_value: Annotated[
        Optional[str], typer.Option(help="Value to fill nulls, is none skip nulls.")
    ] = None,
):
    try:
        if input_path.is_file():
            process_file(
                file_path=input_path,
                output_dir=output_dir,
                extension=extension.value,
                separator=separator.value,
                category_col=category_col,
                verbose=verbose,
                make_dir=make_dir,
                keep_category_col=keep_category_col,
                output_format=output_format.value,
                output_separator=output_separator.value,
                fill_null_value=fill_null_value,
            )

        else:
            files: List[Path] = list_files(
                dir_path=input_path, file_extension=extension.value
            )
            if not files:
                logger.opt(colors=True).error(
                    f"No files found in {input_path} with extension {extension.value}"
                )
                return

            for file_path in tqdm(
                files,
                desc="Processing files",
                colour="yellow",
            ):
                logger.opt(colors=True).info(f"Reading file {file_path}...")
                process_file(
                    file_path=file_path,
                    output_dir=output_dir,
                    extension=extension.value,
                    separator=separator.value,
                    category_col=category_col,
                    verbose=verbose,
                    make_dir=make_dir,
                    keep_category_col=keep_category_col,
                    output_format=output_format.value,
                    output_separator=output_separator.value,
                    fill_null_value=fill_null_value,
                )
        logger.opt(colors=True).info(f"files saved in" f" {output_dir}...")

    except Exception as e:
        logger.opt(colors=True).error(f"{e}.")


if __name__ == "__main__":
    app()
