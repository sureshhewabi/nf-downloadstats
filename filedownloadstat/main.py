import os

import click

from log_file_stat import LogFileStat
from file_download_stat import FileDownloadStat
from parquet_reader import ParquetReader
from stat_parquet import StatParquet
from file_util import FileUtil


@click.command("get_log_files",
               short_help="get_log_files", )
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
@click.option(
    "-p",
    "--protocols",
    help="List of File Download protocols as appeared in the file system (i.e. fasp-aspera/gridftp-globus/http/ftp)",
    required=True,
    type=str
)
def get_log_files(root_dir: str, output: str, protocols: str):
    protocol_list = protocols.split()
    fileutil = FileUtil()
    file_paths_list = fileutil.process_access_methods(root_dir, output, protocol_list)
    return file_paths_list


@click.command("process_log_file",
               short_help="process log_file", )
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
@click.option(
    "-r",
    "--resource",
    help="List of identifiers(paths) in file URIs to identify resources from(Eg: /pride/data/archive)",
    required=True,
    type=str
)
@click.option(
    "-c",
    "--complete",
    help="File download status can be complete or incomplete",
    required=True,
    type=str
)
@click.option(
    "-b",
    "--batch",
    help="Batch size of the TVS file to read",
    required=False,
    type=int
)
def process_log_file(tsvfilepath, output_parquet, resource: str, complete: str, batch: int = 1000):
    resource_list = resource.split()
    completeness_list = complete.split()
    fileutil = FileUtil()
    fileutil.process_log_file(tsvfilepath, output_parquet, resource_list, completeness_list, batch)


@click.command("run_log_file_stat",
               short_help="Run Log file Statistics", )
@click.option(
    "-f",
    "--file",
    help="All the tsv log file paths written to this file",
    required=True,
    type=str
)
@click.option(
    "-o",
    "--output",
    help="Report on HTML",
    required=True,
    type=str
)
def run_log_file_stat(file: str, output: str):
    log_file_stat = LogFileStat()
    log_file_stat.run_log_file_stat(file, output)

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
    "get_file_counts",
    short_help="Get file counts from parquet files",
)
@click.option("-f",
              "--input_dir",
              required=True,
              )
@click.option("-g",
              "--output_grouped",
              required=True,
              )
@click.option("-s",
              "--output_summed",
              required=True,
              )
def get_file_counts(input_dir, output_summed, output_grouped):
    stat_parquet = StatParquet()
    result = stat_parquet.get_file_counts(input_dir, output_grouped, output_summed)
    print(result)


@click.command("run_file_download_stat",
               short_help="Run File Down Statistics", )
@click.option(
    "-f",
    "--file",
    help="JSON file containing file download stats",
    required=True,
    type=str
)
@click.option(
    "-o",
    "--output",
    help="Report on HTML",
    required=True,
    type=str
)
def run_file_download_stat(file: str, output: str):
    file_download_stat = FileDownloadStat()
    file_download_stat.run_file_download_stat(file, output)


@click.group()
def cli():
    pass


# =============== Features Used ===============

cli.add_command(get_log_files)
cli.add_command(run_log_file_stat)
cli.add_command(process_log_file)
cli.add_command(get_file_counts)
cli.add_command(run_file_download_stat)

# =============== Additional Features ===============

cli.add_command(read_parquet_files)

if __name__ == "__main__":
    cli()
