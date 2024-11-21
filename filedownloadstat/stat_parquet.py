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
        grouped = grouped.rename(columns={0: "file_count"})  # Rename the count column to 'file_count'

        # Convert Dask DataFrame to Pandas DataFrame
        grouped = grouped.compute()

        # Save to JSON (grouped counts)
        grouped.to_json(output_grouped, orient='records', lines=True)
        print(f"Grouped counts saved to {output_grouped}")

        # Group by accession and sum file counts
        summed = grouped.groupby("accession", as_index=False)["file_count"].sum()

        # Save to JSON (summed counts)
        summed.to_json(output_summed, orient='records', lines=True)
        print(f"Summed counts saved to {output_summed}")

    def get_all_parquet_files(self, input_files):

        all_parquet_files = []

        # Loop over each directory or file path
        for path in input_files.strip('[]').split(", "):
            path = path.strip("'").strip()  # Clean up any extra spaces or quotes

            # If it's a directory, find all parquet files in that directory and subdirectories
            if os.path.isdir(path):
                all_parquet_files.extend(glob.glob(os.path.join(path, '**', '*.parquet'), recursive=True))
            # If it's a single file, just check if it's a parquet file
            elif os.path.isfile(path) and path.endswith(".parquet"):
                all_parquet_files.append(path)

        if not all_parquet_files:
            raise FileNotFoundError("No Parquet files found in the provided directories or file paths.")

        return all_parquet_files

