import logging
import pyarrow.parquet as pq

from exceptions import (
    ParquetReadError,
    ValidationError
)

logger = logging.getLogger(__name__)


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
            raise ValidationError("parquet_path is required", field="parquet_path")

        try:
            # Read the dataset (directory of Parquet files)
            read_table = pq.read_table(parquet_path)
        except (IOError, OSError, FileNotFoundError) as e:
            error = ParquetReadError(
                f"Failed to read Parquet file: {parquet_path}",
                parquet_path=parquet_path,
                original_error=str(e)
            )
            logger.error("Error reading Parquet file", extra={"parquet_path": parquet_path, "error": str(e)}, exc_info=True)
            raise error
        except Exception as e:
            error = ParquetReadError(
                f"Unexpected error reading Parquet file: {parquet_path}",
                parquet_path=parquet_path,
                original_error=str(e)
            )
            logger.error("Unexpected error reading Parquet file", extra={"parquet_path": parquet_path, "error": str(e)}, exc_info=True)
            raise error

        # Log metadata
        logger.info("Parquet file read successfully", extra={"parquet_path": parquet_path})
        logger.debug("Parquet metadata", extra={"metadata": str(read_table.schema.metadata), "schema": str(read_table.schema)})
        logger.debug("Parquet data preview", extra={"row_count": len(read_table.to_pandas())})

        return read_table
