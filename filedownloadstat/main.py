import os
import click

from parquet_reader import ParquetReader
from stat_parquet import StatParquet
from file_util import FileUtil

# access_methods_folder_names = ['fasp-aspera', 'gridftp-globus', 'http', 'ftp']


access_methods_folder_names = ['ftp']

@click.command("get_log_files",
    short_help="get_log_files",)
@click.option(
    "-r",
    "--root_dir",
    help="root folder where all the log files are stored(i.e transfer_logs_privacy_ready)",
    required=True,
)
@click.option(
    "-o",
    "--output",
    help="all the tsv log file paths written to this file",
    required=True,
)
def get_log_files(root_dir: str, output: str):
    fileutil = FileUtil()
    file_paths_list = fileutil.process_access_methods(root_dir, access_methods_folder_names, output)
    return file_paths_list


@click.command("process_log_file",
    short_help="process log_file",)
@click.option(
    "-f",
    "--tsvfilepath",
    help="all the tsv log file paths written to this file",
    required=True,
)
@click.option(
    "-o",
    "--output_parquet",
    help="parquet file path to write individual file",
    required=True,
)
def process_log_file(tsvfilepath, output_parquet):
    fileutil = FileUtil()
    fileutil.process_log_file(tsvfilepath, output_parquet)


@click.command(
    "read-parquet",
    short_help="Read a single parquet file",
)
@click.option(
    "--file",
    help="Single parquet file to read",
    required=True,
)
def read_parquet_files(file: str):
    if os.path.exists(file):
        parquet_reader = ParquetReader(file)
        parquet_reader.read(file)
    else:
        raise FileNotFoundError(file)


@click.command(
    "stat",
    short_help="Generate stats from all the parquet files",
)
@click.option("-f",
              "--file",
              help="Parquet file to generate stats",
              required=True,
              )
@click.option("-o",
              "--output",
              help="Generated stats file",
              required=True,
              )
def get_stat_from_parquet_files(file, output: str):
    stat_parquet = StatParquet()
    result = stat_parquet.count_records_by_accession(file, output)
    print(result)


@click.group()
def cli():
    pass


cli.add_command(get_log_files)
cli.add_command(process_log_file)
cli.add_command(read_parquet_files)
cli.add_command(get_stat_from_parquet_files)

if __name__ == "__main__":
    cli()
