import gzip
import os
import sys
from pathlib import Path
from log_parser import LogParser
from parquet_writer import ParquetWriter


class FileUtil:

    def __init__(self):
        pass

    def get_file_paths(self, root_dir):
        """
        Traverse the directory tree and retrieve all file paths.
        Handles variable folder depth safely using pathlib.
        :param root_dir: Root directory to start traversal.
        :return: List of file paths.
        """
        root_path = Path(root_dir)
        return [str(file) for file in root_path.rglob("*.tsv.gz")]


    def count_lines_in_gz(self, file_path):
        """
        Count the number of lines in a gzipped file.
        :param file_path: Path to the gzipped file.
        :return: Number of lines.
        """
        line_count = 0
        try:
            with gzip.open(file_path, "rt", encoding="utf-8") as gz_file:
                for _ in gz_file:
                    line_count += 1
        except Exception as e:
            print(f"Error counting lines in {file_path}: {e}")
        return line_count


    def process_access_methods(self, root_directory: str, file_paths_list: str, protocols: list):
        """
        Process logs and generate Parquet files for each file in the specified access method directories.
        """

        file_metadata = []

        for protocol in protocols:
            method_directory = Path(root_directory) / protocol / "public"
            files = self.get_file_paths(str(method_directory))

            for file_path in files:
                file_path_obj = Path(file_path)

                # Retrieve file size
                file_size = file_path_obj.stat().st_size

                # Count number of lines in the gzipped file
                line_count = self.count_lines_in_gz(file_path)

                file_info = {
                    "path": file_path,
                    "filename": file_path_obj.name,
                    "size": file_size,
                    "lines": line_count,
                }
                file_metadata.append(file_info)

            # Write metadata to the output file
        with open(file_paths_list, "w") as f:
            for metadata in file_metadata:
                f.write(
                    f"{metadata['path']}\t{metadata['filename']}\t{metadata['size']}\t{metadata['lines']}\n"
                )

        print(f"File metadata written to {file_paths_list}")
        return file_paths_list

    def process_log_file(self, file_path: str, parquet_output_file: str, resource_list: list, completeness_list: list, batch_size: int):
        data_written = False
        try:
            print(f"Parsing log file started: {file_path}")

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Input file does not exist: {file_path}")

            print(f"Parsing file and writing output to {parquet_output_file}")

            lp = LogParser(file_path, resource_list, completeness_list)
            writer = ParquetWriter(parquet_path=parquet_output_file, write_strategy='batch', batch_size=batch_size)

            for batch in lp.parse_gzipped_tsv(batch_size=batch_size):
                if writer.write_batch(batch):
                    data_written = True

            # Finalize and check if any data was written
            if writer.finalize():
                data_written = True

            if data_written:
                print(f"Parquet file written to {parquet_output_file} for {file_path}")
            else:
                print(f"No data found to write :  {file_path}")
        except Exception as e:
            print(f"Error while processing file: {e}", file=sys.stderr)
            sys.exit(1)
