import os
import pandas as pd
import dask.dataframe as dd
from scipy.stats import rankdata
from dask.distributed import Client, progress
from dask_jobqueue import SLURMCluster
from dask.distributed import LocalCluster
import plotly.express as px
import panel as pn


def setup_dask_cluster(profile="slurm"):
    """Setup a Dask distributed client for Slurm or Local based on the profile."""
    if profile == "ebislurm":
        cluster = SLURMCluster(cores=8,
                               memory='32GB',
                               queue='standard',
                               walltime='01:00:00',
                               job_extra_directives=['-o dask_job.%j.%N.out', '-e dask_job.%j.%N.error'])
        cluster.scale(jobs=10)  # Scale to 10 nodes (adjust as needed)
        cluster.adapt(maximum_jobs=20)
    else:
        cluster = LocalCluster()

    client = Client(cluster, timeout="1800s")

    print(f"Dask Dashboard running at: {client.dashboard_link}")
    # Optionally, use Panel to create a report
    pn.panel(client).save("dask_dashboard.html")

    print(f"Dask cluster: {cluster}")
    print(f"Dask Client connected: {client}")
    return client, cluster


class ParquetAnalyzer:
    def __init__(self, profile="ebislurm"):
        self.client, self.cluster = setup_dask_cluster(profile)

    def analyze_parquet_files(self,
                              output_parquet,
                              project_level_download_counts,
                              file_level_download_counts,
                              project_level_yearly_download_counts,
                              project_level_top_download_counts,
                              all_data):
        # Only load the necessary columns instead of reading the entire Parquet file.
        ddf = dd.read_parquet(output_parquet, engine="pyarrow", columns=["accession", "filename", "year"],
                              optimize_read=True)
        ddf = ddf.repartition(npartitions=50)
        ddf = ddf.persist()  # Distributes work across the cluster and returns a lazy Dask object
        progress(ddf)

        # Process and save different statistics
        self.persist_project_level_download_counts(ddf, project_level_download_counts)
        self.persist_file_level_download_counts(ddf, file_level_download_counts)
        self.persist_project_level_yearly_download_counts(ddf, project_level_yearly_download_counts)
        self.persist_top_download_counts(ddf, project_level_top_download_counts, top_counts=100)
        self.persist_all_data(ddf, all_data)

    def persist_project_level_download_counts(self, ddf, project_level_download_counts):
        # Group by accession and count the number of files
        project_level_counts = ddf.groupby("accession")["filename"].count().reset_index()

        project_level_counts = project_level_counts.repartition(npartitions=10).persist()
        progress(project_level_counts)
        result = project_level_counts.compute()
        result["percentile"] = (rankdata(result["filename"], method="average") / len(result) * 100).astype(int)
        result.sort_values(by="filename", ascending=False).to_json(project_level_download_counts, orient="records",
                                                                   lines=False)
        print(f"{project_level_download_counts} file saved successfully!")

    def persist_file_level_download_counts(self, ddf, file_level_download_counts):
        # Group by 'accession' and 'filename', then count occurrences
        file_counts = ddf.groupby(["accession", "filename"]).size().reset_index()

        # Rename columns
        file_counts.columns = ["accession", "filename", "count"]

        file_counts = file_counts.persist()
        progress(file_counts)
        result = file_counts.compute()

        # Save to JSON
        result.to_json(file_level_download_counts, orient="records", lines=False)

        print(f"{file_level_download_counts} file saved successfully!")

    def persist_project_level_yearly_download_counts(self, ddf, project_level_yearly_download_counts):
        yearly_counts = ddf.groupby(["accession", "year"]).size().reset_index()
        yearly_counts.columns = ["accession", "year", "count"]
        yearly_counts = yearly_counts.persist()
        progress(yearly_counts)
        result = yearly_counts.compute()
        grouped = result.groupby("accession").apply(lambda x: {"accession": x["accession"].iloc[0],
                                                               "yearlyDownloads": x[["year", "count"]].to_dict(
                                                                   orient="records")}).tolist()
        pd.DataFrame(grouped).to_json(project_level_yearly_download_counts, orient="records", lines=False)
        print(f"{project_level_yearly_download_counts} file saved successfully!")

    def persist_top_download_counts(self, ddf, project_level_top_download_counts, top_counts=100):
        file_counts = ddf.groupby("accession").size().reset_index()
        file_counts.columns = ["accession", "count"]
        file_counts = file_counts.persist()
        progress(file_counts)
        result = file_counts.compute()
        top_count_dataset = result.sort_values("count", ascending=False).head(top_counts)
        top_count_dataset.to_json(project_level_top_download_counts, orient="records", lines=False)
        print(f"{project_level_top_download_counts} file saved successfully!")

    def persist_all_data(self, ddf, all_data):
        ddf = ddf.persist()  # Distributes work across the cluster and returns a lazy Dask object
        progress(ddf)  # Monitor distributed execution
        df = ddf.compute()  # Collects final results
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
        all_files = self.get_all_parquet_files(input_files)
        if not all_files:
            print("No valid Parquet files found. Exiting.")
            return
        print(f"Loading {len(all_files)} Parquet files...")
        ddf = dd.read_parquet(all_files, engine="pyarrow")
        ddf = ddf.persist()
        progress(ddf)
        ddf.to_parquet(output_parquet, engine="pyarrow", write_index=False, overwrite=True)
        print(f"Merged Parquet dataset saved at: {output_parquet}")

    def close_cluster(self):
        """Shut down the Dask cluster properly."""
        print("Closing Dask cluster...")
        self.client.close()  # Closes the client connection
        self.cluster.close()  # Shuts down the cluster
        print("Dask cluster closed successfully.")
