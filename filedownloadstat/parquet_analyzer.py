# import os
# import pandas as pd
# import dask.dataframe as dd
# from scipy.stats import rankdata
# from dask.distributed import progress
#
# class ParquetAnalyzer:
#     def __init__(self, dask_manager ):
#         """Initialize with an existing DaskManager instance."""
#         self.dask_manager = dask_manager  # Inject DaskManager dependency
#
#     def analyze_parquet_files(self,
#                               output_parquet,
#                               project_level_download_counts,
#                               file_level_download_counts,
#                               project_level_yearly_download_counts,
#                               project_level_top_download_counts,
#                               all_data):
#         # Only load the necessary columns instead of reading the entire Parquet file.
#         ddf = dd.read_parquet(output_parquet, engine="pyarrow", columns=["accession", "filename", "year"],
#                               optimize_read=True)
#         ddf = ddf.repartition(npartitions=50)
#         ddf = ddf.persist()  # Distributes work across the cluster and returns a lazy Dask object
#         progress(ddf)
#
#         # Process and save different statistics
#         self.persist_project_level_download_counts(ddf, project_level_download_counts)
#         self.persist_file_level_download_counts(ddf, file_level_download_counts)
#         self.persist_project_level_yearly_download_counts(ddf, project_level_yearly_download_counts)
#         self.persist_top_download_counts(ddf, project_level_top_download_counts, top_counts=100)
#         self.persist_all_data(ddf, all_data)
#
#     def persist_project_level_download_counts(self, ddf, project_level_download_counts):
#         # Group by accession and count the number of files
#         project_level_counts = ddf.groupby("accession")["filename"].count().reset_index()
#
#         project_level_counts = project_level_counts.repartition(npartitions=10).persist()
#         result = project_level_counts.compute()
#         result["percentile"] = (rankdata(result["filename"], method="average") / len(result) * 100).astype(int)
#         result.sort_values(by="filename", ascending=False).to_json(project_level_download_counts, orient="records",
#                                                                    lines=False)
#         print(f"{project_level_download_counts} file saved successfully!")
#
#     def persist_file_level_download_counts(self, ddf, file_level_download_counts):
#         # Group by 'accession' and 'filename', then count occurrences
#         file_counts = ddf.groupby(["accession", "filename"]).size().reset_index()
#
#         # Rename columns
#         file_counts.columns = ["accession", "filename", "count"]
#
#         file_counts = file_counts.persist()
#         result = file_counts.compute()
#
#         # Save to JSON
#         result.to_json(file_level_download_counts, orient="records", lines=False)
#
#         print(f"{file_level_download_counts} file saved successfully!")
#
#     def persist_project_level_yearly_download_counts(self, ddf, project_level_yearly_download_counts):
#         yearly_counts = ddf.groupby(["accession", "year"]).size().reset_index()
#         yearly_counts.columns = ["accession", "year", "count"]
#         yearly_counts = yearly_counts.persist()
#         result = yearly_counts.compute()
#         grouped = result.groupby("accession").apply(lambda x: {"accession": x["accession"].iloc[0],
#                                                                "yearlyDownloads": x[["year", "count"]].to_dict(
#                                                                    orient="records")}).tolist()
#         pd.DataFrame(grouped).to_json(project_level_yearly_download_counts, orient="records", lines=False)
#         print(f"{project_level_yearly_download_counts} file saved successfully!")
#
#     def persist_top_download_counts(self, ddf, project_level_top_download_counts, top_counts=100):
#         file_counts = ddf.groupby("accession").size().reset_index()
#         file_counts.columns = ["accession", "count"]
#         file_counts = file_counts.persist()
#         result = file_counts.compute()
#         top_count_dataset = result.sort_values("count", ascending=False).head(top_counts)
#         top_count_dataset.to_json(project_level_top_download_counts, orient="records", lines=False)
#         print(f"{project_level_top_download_counts} file saved successfully!")
#
#     def persist_all_data(self, ddf, all_data):
#         df = ddf.compute()  # Collects final results
#         df.to_json(all_data, orient="records", lines=False)
#         print(f"All data saved to {all_data}")
#
#     def get_all_parquet_files(self, file_list_path):
#         """Read file paths from a text file and validate them as Parquet files."""
#         all_parquet_files = []
#
#         # Read the file list
#         with open(file_list_path, "r") as f:
#             file_paths = f.readlines()
#
#         for path in file_paths:
#             path = path.strip()
#
#             # Check if the path is a valid Parquet file or directory
#             if os.path.isfile(path) and path.endswith(".parquet"):
#                 all_parquet_files.append(path)
#             elif os.path.isdir(path):
#                 dataset_files = [
#                     os.path.join(path, file)
#                     for file in os.listdir(path)
#                     if file.endswith(".parquet")
#                 ]
#                 all_parquet_files.extend(dataset_files)
#
#         if not all_parquet_files:
#             print("Warning: No valid Parquet files found in the provided paths.")
#             return []
#
#         return all_parquet_files
#
#     def merge_parquet_files(self, input_files, output_parquet):
#         all_files = self.get_all_parquet_files(input_files)
#         if not all_files:
#             print("No valid Parquet files found. Exiting.")
#             return
#         print(f"Loading {len(all_files)} Parquet files...")
#         ddf = dd.read_parquet(all_files, engine="pyarrow")
#         ddf = ddf.persist()
#         progress(ddf)
#         ddf.to_parquet(output_parquet, engine="pyarrow", write_index=False, overwrite=True)
#         print(f"Merged Parquet dataset saved at: {output_parquet}")

import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from scipy.stats import rankdata


class ParquetAnalyzer:
    def __init__(self, batch_size=100_000):
        """Initialize with a batch size for processing."""
        self.batch_size = int(batch_size)  # Number of rows to process at a time

    def analyze_parquet_files(self, output_parquet, project_level_download_counts,
                              file_level_download_counts, project_level_yearly_download_counts,
                              project_level_top_download_counts, all_data):
        """Processes Parquet files in batches without using Dask."""
        file_iter = pq.ParquetFile(output_parquet).iter_batches(batch_size=self.batch_size,
                                                                columns=["accession", "filename", "year"])

        project_counts = []
        file_counts = []
        yearly_counts = []

        for batch in file_iter:
            df = batch.to_pandas()

            # Aggregate project-level counts
            project_counts.append(df.groupby("accession")["filename"].count().reset_index())

            # Aggregate file-level counts
            file_counts.append(df.groupby(["accession", "filename"]).size().reset_index(name="count"))

            # Aggregate yearly counts
            yearly_counts.append(df.groupby(["accession", "year"]).size().reset_index(name="count"))

        # Combine results
        self.persist_project_level_download_counts(pd.concat(project_counts, ignore_index=True),
                                                   project_level_download_counts)
        self.persist_file_level_download_counts(pd.concat(file_counts, ignore_index=True), file_level_download_counts)
        self.persist_project_level_yearly_download_counts(pd.concat(yearly_counts, ignore_index=True),
                                                          project_level_yearly_download_counts)
        self.persist_top_download_counts(output_parquet, project_level_top_download_counts, top_counts=100)

        # Save full dataset incrementally
        self.persist_all_data(output_parquet, all_data)

    def persist_project_level_download_counts(self, df, output_file):
        df["percentile"] = (rankdata(df["filename"], method="average") / len(df) * 100).astype(int)
        df.sort_values(by="filename", ascending=False).to_json(output_file, orient="records", lines=False)
        print(f"{output_file} file saved successfully!")

    def persist_file_level_download_counts(self, df, output_file):
        df.to_json(output_file, orient="records", lines=False)
        print(f"{output_file} file saved successfully!")

    def persist_project_level_yearly_download_counts(self, df, output_file):
        grouped = df.groupby("accession").apply(lambda x: {
            "accession": x["accession"].iloc[0],
            "yearlyDownloads": x[["year", "count"]].to_dict(orient="records")
        }).tolist()
        pd.DataFrame(grouped).to_json(output_file, orient="records", lines=False)
        print(f"{output_file} file saved successfully!")

    def persist_top_download_counts(self, input_parquet, output_file, top_counts=100):
        """Processes Parquet file batch-wise to determine top downloads."""
        file_iter = pq.ParquetFile(input_parquet).iter_batches(batch_size=self.batch_size, columns=["accession"])
        counts = {}

        for batch in file_iter:
            df = batch.to_pandas()
            for acc in df["accession"]:
                counts[acc] = counts.get(acc, 0) + 1

        result = pd.DataFrame(list(counts.items()), columns=["accession", "count"])
        top_count_dataset = result.sort_values("count", ascending=False).head(top_counts)
        top_count_dataset.to_json(output_file, orient="records", lines=False)
        print(f"{output_file} file saved successfully!")

    def persist_all_data(self, input_parquet, output_file):
        """Reads and saves all data batch-wise to avoid memory issues."""
        file_iter = pq.ParquetFile(input_parquet).iter_batches(batch_size=self.batch_size)

        with open(output_file, "w") as f:
            for batch in file_iter:
                df = batch.to_pandas()
                f.write(df.to_json(orient="records", lines=True))  # **Append JSON records batch-wise**
        print(f"All data saved to {output_file}")

    def get_all_parquet_files(self, file_list_path):
        """Reads file paths from a text file and validates them as Parquet files."""
        with open(file_list_path, "r") as f:
            file_paths = [line.strip() for line in f.readlines()]

        all_parquet_files = [p for p in file_paths if os.path.isfile(p) and p.endswith(".parquet")]
        return all_parquet_files if all_parquet_files else print("Warning: No valid Parquet files found.")

    def merge_parquet_files(self, input_files, output_parquet):
        """Merges Parquet files in batches with schema consistency."""
        all_files = self.get_all_parquet_files(input_files)
        if not all_files:
            print("No valid Parquet files found. Exiting.")
            return

        writer = None
        first_schema = None

        for file in all_files:
            file_iter = pq.ParquetFile(file).iter_batches(batch_size=self.batch_size)
            for batch in file_iter:
                df = batch.to_pandas()

                if writer is None:
                    first_schema = pa.Table.from_pandas(df).schema  # Capture schema from first batch
                    writer = pq.ParquetWriter(output_parquet, first_schema)

                table = pa.Table.from_pandas(df, schema=first_schema)  # Ensure schema consistency
                writer.write_table(table)

        if writer:
            writer.close()
        print(f"Merged Parquet dataset saved at: {output_parquet}")
