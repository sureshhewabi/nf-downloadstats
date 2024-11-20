from pathlib import Path
from log_parser import LogParser
from parquet_writer import ParquetWriter


class FileUtil:

    def get_file_paths(self, root_dir):
        """
        Traverse the directory tree and retrieve all file paths.
        Handles variable folder depth safely using pathlib.
        :param root_dir: Root directory to start traversal.
        :return: List of file paths.
        """
        root_path = Path(root_dir)
        return [str(file) for file in root_path.rglob("*.tsv.gz")]

    def process_access_methods(self, root_directory, access_methods_folder_names, parquet_output_file):
        """
        Process logs and generate Parquet files for each file in the specified access method directories.
        """
        for access_method in access_methods_folder_names:
            method_directory = Path(root_directory) / access_method / "public"
            files = self.get_file_paths(str(method_directory))

            for file_path in files:
                # Parse logs
                lp = LogParser(file_path)
                data = lp.parse_gzipped_tsv()

                # Write to Parquet
                parquet_writer = ParquetWriter(parquet_output_file)
                parquet_writer.write(data, parquet_output_file)

                print(f"Parquet file written to {parquet_output_file} for {file_path}")
