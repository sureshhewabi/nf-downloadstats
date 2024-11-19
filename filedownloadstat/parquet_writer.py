import pyarrow.parquet as pq
import pyarrow as pa


class ParquetWriter:
    """
    Write parquet file
    """
    # Define schema with metadata
    schema = pa.schema([
        pa.field('date', pa.date64(), metadata={'description': 'Date that the dataset was downloaded'}),
        pa.field('year', pa.int16(), metadata={'description': 'Year that the dataset was downloaded'}),
        pa.field('month', pa.int8(), metadata={'description': 'Month that the dataset was downloaded'}),
        pa.field('accession', pa.string(), metadata={'description': 'PRIDE accession started with PXD'}),
        pa.field('filename', pa.string(), metadata={'description': 'Filename of the file downloaded'}),
        pa.field('completed', pa.string(), metadata={'description': 'Check if the file download was completed'}),
        pa.field('country', pa.string(), metadata={'description': 'Country of the file downloaded'}),
        pa.field('method', pa.string(), metadata={'description': 'Download method such as FTP/Aspera/Globus'}),
    ])

    def __init__(self, parquet_path: str = None):
        self.parquet_path = parquet_path

    def write(self, data, parquet_output):
        """
        Write parquet file with schema
        :param parquet_output:
        :param data: array of data to write
        :return:
        """
        if parquet_output is None:
            raise ValueError("parquet_output is required")
        if parquet_output is None:
            parquet_output = self.parquet_path

        COMPRESSION = 'snappy'

        try:
            # Convert data to PyArrow Table
            table = pa.Table.from_pylist(data, schema=self.schema)
            if table is not None and len(table) > 0:
                # Write to a Parquet dataset with partitioning
                pq.write_to_dataset(
                    table,
                    root_path=parquet_output,
                    # partition_cols=['year', 'month'],  # Specify partition columns
                    compression=COMPRESSION)
            else:
                print("No data found to write!")
        except Exception as e:
            raise ValueError(e.with_traceback())
