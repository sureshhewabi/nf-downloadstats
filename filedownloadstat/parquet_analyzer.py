import os
import dask.dataframe as dd
from scipy.stats import rankdata

class ParquetAnalyzer:
    def __init__(self):
        pass

    def analyze_parquet_files(self,
                        output_parquet,
                        project_level_download_counts,
                        file_level_download_counts,
                        project_level_yearly_download_counts,
                        project_level_top_download_counts,
                        all_data):

        ddf = dd.read_parquet(output_parquet, engine="pyarrow")

        # Process and save different statistics
        self.persist_project_level_download_counts(ddf, project_level_download_counts)
        self.persist_file_level_download_counts(ddf, file_level_download_counts)
        self.persist_project_level_yearly_download_counts(ddf, project_level_yearly_download_counts)
        self.persist_top_download_counts(ddf, project_level_top_download_counts, top_counts=100)
        self.persist_all_data(ddf, all_data)

    def persist_project_level_download_counts(self, ddf, project_level_download_counts):
        # Group by accession and count the number of files
        project_level_counts = ddf.groupby("accession")["filename"].count().reset_index()

        # Rename columns
        project_level_counts.columns = ["accession", "count"]

        # Compute the result
        result = project_level_counts.compute()

        # Calculate percentile rank for the 'count' column
        result["percentile"] = (rankdata(result["count"], method="average") / len(result) * 100).astype(int)

        # Sort by count in descending order
        result = result.sort_values(by="count", ascending=False)

        # Save to JSON
        result.to_json(project_level_download_counts, orient="records", lines=False)

        print(f"{project_level_download_counts} file saved successfully!")

    def persist_file_level_download_counts(self, ddf, file_level_download_counts):
        # Group by 'accession' and 'filename', then count occurrences
        file_counts = ddf.groupby(["accession", "filename"]).size().reset_index()

        # Rename columns
        file_counts.columns = ["accession", "filename", "count"]

        # Compute the result
        result = file_counts.compute()

        # Save to JSON
        result.to_json(file_level_download_counts, orient="records", lines=False)

        print(f"{file_level_download_counts} file saved successfully!")

    def persist_project_level_yearly_download_counts(self, ddf, project_level_yearly_download_counts):
        # Group by 'accession' and 'year', then count occurrences
        file_counts = ddf.groupby(["accession", "year"]).size().reset_index()

        # Rename columns
        file_counts.columns = ["accession", "year", "count"]

        # Compute the result
        result = file_counts.compute()

        # Save to JSON
        result.to_json(project_level_yearly_download_counts, orient="records", lines=False)

        print(f"{project_level_yearly_download_counts} file saved successfully!")

    def persist_top_download_counts(self, ddf, project_level_top_download_counts, top_counts=100):
        # Group by 'accession' and count occurrences
        file_counts = ddf.groupby("accession").size().reset_index()

        # Rename columns
        file_counts.columns = ["accession", "count"]

        # Compute first, then sort and get top 100 records
        file_counts_df = file_counts.compute()
        top_count_dataset = file_counts_df.sort_values("count", ascending=False).head(top_counts)

        # Save to JSON
        top_count_dataset.to_json(project_level_top_download_counts, orient="records", lines=False)

        print(f"{project_level_top_download_counts} file saved successfully!")

    def persist_all_data(self, ddf, all_data):
        # Convert to a pandas DataFrame
        df = ddf.compute()

        # Save to a single JSON file
        df.to_json(all_data, orient="records", lines=False)
        print(f"All data saved to {all_data}")

    def get_all_parquet_files(self, file_list_path):
        """Read file paths from a text file and validate them as Parquet files."""
        all_parquet_files = []

        # Read the file list
        with open(file_list_path, "r") as f:
            file_paths = f.readlines()

        for path in file_paths:
            path = path.strip()

            # Check if the path is a valid Parquet file or directory
            if os.path.isfile(path) and path.endswith(".parquet"):
                all_parquet_files.append(path)
            elif os.path.isdir(path):
                dataset_files = [
                    os.path.join(path, file)
                    for file in os.listdir(path)
                    if file.endswith(".parquet")
                ]
                all_parquet_files.extend(dataset_files)

        if not all_parquet_files:
            print("Warning: No valid Parquet files found in the provided paths.")
            return []

        return all_parquet_files

    def merge_parquet_files(self, input_files, output_parquet):
        """
        Reads multiple Parquet files, merges them in a memory-efficient way, and saves them.
        """
        # Get all valid Parquet files
        all_files = self.get_all_parquet_files(input_files)
        if not all_files:
            print("No valid Parquet files found. Exiting.")
            return

        print(f"Loading {len(all_files)} Parquet files...")

        # Read all Parquet files into a Dask DataFrame (Lazy loading)
        ddf = dd.read_parquet(all_files, engine="pyarrow")

        # Write to a partitioned Parquet dataset (memory-efficient)
        ddf.to_parquet(output_parquet, engine="pyarrow", write_index=False, overwrite=True)

        print(f"Merged Parquet dataset saved at: {output_parquet}")
