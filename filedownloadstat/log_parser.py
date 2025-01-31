import gzip
import re
from datetime import datetime
import warnings

# Suppress specific warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="dask.dataframe")


class LogParser:
    """
    Class to parse the log file into parquet format
    """

    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

    def __init__(self, file_path, resource_list, completeness_list, accession_pattern: str):
        self.file_path = file_path
        self.RESOURCE_IDENTIFIERS = resource_list
        self.completeness = {c.lower().strip() for c in completeness_list}
        self.accession_pattern = accession_pattern

    def parse_gzipped_tsv(self, batch_size: int ):
        """
        Read the gzipped TSV file, parse each line, and yield data in batches.
        :param batch_size: Number of rows to include in each batch.
        :return: Generator that yields batches of parsed data.
        """
        batch = []
        try:
            with gzip.open(self.file_path, "rt", encoding="utf-8") as log_file:
                for line_no, line in enumerate(log_file, start=1):
                    line = line.replace('\\t', '\t')  # Replace literal '\t' with actual tab
                    row = line.strip().split('\t')  # Split each line by tab
                    parsed_line = self.parse_row(row, line_no)
                    if parsed_line:
                        batch.append(parsed_line)
                        # Yield the batch when it reaches the desired size
                        if len(batch) == batch_size:
                            yield batch
                            batch = []  # Reset the batch after yielding
                if batch:
                    yield batch
        except OSError as e:
            print(f"Skipping corrupted file: {self.file_path} due to {e}")
        except Exception as e:
            print(f"Got an exception while processing file {self.file_path}: {e}")

    def is_relevant_row(self, row):
        """
        Checks if the row matches the criteria based on resource identifiers and completeness.
        :param row: List of row values
        :return: Boolean indicating if the row is relevant
        """
        if  '/' in row[3]:
            accession_field = row[3].strip().split('/')[-2]
            accession_regex = re.compile(self.accession_pattern)

            return (
                    row[6].lower().strip() in self.completeness and  # complete and/or partial download
                    any(row[3].startswith(identifier) for identifier in self.RESOURCE_IDENTIFIERS) and
                    bool(accession_regex.match(accession_field)) and  # should follow accession patterns
                    row[3].split('/')[-1] is not None and # filename cannot be null
                    row[3].split('/')[-1] != "" # filename cannot be null
            )
        else:
            # print(f"No file path found in "+ str(row))
            return False

    def parse_row(self, row, line_no):
        """
        Define a function to parse each row by extracting fields by column index
        :param row:
        :param line_no:
        :return:
        """
        if len(row) == 13:
            if self.is_relevant_row(row):
                try:
                    timestamp = self.clean_timestamp(row[0])
                    parsed_time = datetime.strptime(timestamp, self.DATETIME_FORMAT)

                    # Extract year, month, and date
                    year = parsed_time.year
                    month = parsed_time.month
                    date = parsed_time.date()

                    return {
                        "date": date,  # Date
                        "year": year,
                        "month": month,
                        "user": row[1].strip(),
                        "accession": row[3].split('/')[-2],  # Project accession of the resource(eg: PXD accession in PRIDE)
                        "filename": row[3].split('/')[-1],  # Files that are associate to a project(project acceesion)
                        "completed": row[6].lower().strip(),  # Completion Status (e.g., Complete or Incomplete)
                        "country": row[7],  # Country
                        "method": row[11],  # Method (e.g., ftp, aspera)
                    }
                except IndexError as e:
                    print(f"Error processing line {line_no} with row {row}: {e}")
                    raise IndexError(f"IndexError: Row {line_no} with insufficient columns: {row}. Error: {e}")
            else:
                # print(f"Row not relevant at line {line_no}: {row}")
                return None
        else:
            print(f"WARNING: Expected 13 columns but found {len(row)} columns at line {line_no}. Row: {row}")
            return None

    @staticmethod
    def clean_timestamp(timestamp):
        """
        Cleans and adjusts the timestamp format for parsing.
        eg: 2024-09-13T23:58:17.000Z / 2024-09-14T07:14:07.419698061Z
        :param timestamp: Raw timestamp string
        :return: Cleaned timestamp string
        """
        if '.' in timestamp:
            timestamp = timestamp[:26] + 'Z'  # Trim to microseconds and re-add 'Z'
        return timestamp.rstrip('Z')  # Remove 'Z' for parsing
