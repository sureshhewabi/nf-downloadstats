import glob
import dask.dataframe as dd
import os


class StatParquet:

    def __init__(self):
        pass

    def get_file_counts(self, input_files, output_grouped, output_summed):

        all_files = self.get_all_parquet_files(input_files)
        # Load all parquet files into a Dask DataFrame
        print(f"Loading {len(all_files)} Parquet files...")
        ddf = dd.read_parquet(all_files)

        # Group by accession and filename to count occurrences
        grouped = ddf.groupby(["accession", "filename"]).size().reset_index()
        grouped = grouped.rename(columns={0: "count"})  # Rename the count column to 'count'

        # Convert Dask DataFrame to Pandas DataFrame
        grouped = grouped.compute()

        # Save to JSON (grouped counts)
        grouped.to_json(output_grouped, orient='records', lines=False)
        print(f"Grouped counts saved to {output_grouped}")

        # Group by accession and sum file counts
        summed = grouped.groupby("accession", as_index=False)["count"].sum()

        # Save to JSON (summed counts)
        summed.to_json(output_summed, orient='records', lines=False)
        print(f"Summed counts saved to {output_summed}")

    def get_all_parquet_files(self, file_list_path):
        """Read file paths from a text file and validate them as Parquet files."""
        all_parquet_files = []

        # Read the file list
        with open(file_list_path, 'r') as f:
            file_paths = f.readlines()

        for path in file_paths:
            path = path.strip()

            # Check if the path is a valid Parquet file or directory
            if os.path.isfile(path) and path.endswith('.parquet'):
                all_parquet_files.append(path)
            elif os.path.isdir(path):
                # Handle directory containing Parquet dataset
                dataset_files = [
                    os.path.join(path, file)
                    for file in os.listdir(path)
                    if file.endswith('.parquet')
                ]
                all_parquet_files.extend(dataset_files)

        if not all_parquet_files:
            raise FileNotFoundError("No valid Parquet files found in the provided paths.")

        return all_parquet_files

