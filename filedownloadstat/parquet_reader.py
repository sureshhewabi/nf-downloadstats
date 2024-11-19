import pyarrow.parquet as pq


class ParquetReader:
    """
    Read parquet file
    """

    def __init__(self, parquet_path: str = None):
        self.parquet_path = parquet_path

    def read(self, parquet_path):
        """
        Read parquet file or dataset
        :param parquet_path: Path to the Parquet file or dataset
        :return: DataFrame or Table
        """
        if parquet_path is None:
            raise ValueError("parquet_path is required")

        # Read the dataset (directory of Parquet files)
        read_table = pq.read_table(parquet_path)

        # Print metadata
        print("Metadata:", read_table.schema.metadata)
        print("Schema:", read_table.schema)
        print("Data:")
        print(read_table.to_pandas())  # Convert to pandas DataFrame for easier inspection

        return read_table
