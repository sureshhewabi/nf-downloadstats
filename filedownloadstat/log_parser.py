import gzip
import sys
from datetime import datetime
import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="dask.dataframe")


class LogParser:
    """
    Class to parse the log file into parquet format
    """

    def __init__(self, file_path, resource_list, completeness_list):
        self.file_path = file_path
        self.RESOURCE_IDENTIFIERS = resource_list
        self.completeness = completeness_list

    def parse_gzipped_tsv(self):
        """
        Read the gzipped TSV file and parse each line
        :return:
        """
        parsed_data = []

        try:
            with gzip.open(self.file_path, "rt", encoding="utf-8") as log_file:
                line_no = 0
                for line in log_file:
                    line_no += 1
                    line = line.replace('\\t', '\t')  # Replace literal '\t' with actual tab
                    row = line.strip().split('\t')  # Split each line by tab
                    if len(row) == 13:
                        if any(row[3].startswith(identifier) for identifier in self.RESOURCE_IDENTIFIERS):
                            if any(row[6].lower().strip() == comp for comp in self.completeness):
                                parsed_line = self.parse_row(row)
                                if parsed_line:
                                    parsed_data.append(parsed_line)
                    else:
                        print("WARNING----!!! Number of columns expected was 13 found ", len(row), " in line no ",
                                        line_no, " and row is :", row)
        except OSError as e:
            print(f"Error reading file: {e}")
            return None
        except Exception as e:
            raise e.with_traceback(sys.exc_info()[2])
        return parsed_data

    def parse_row(self, row):
        """
        Define a function to parse each row by extracting fields by column index
        :param row:
        :return:
        """
        DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
        try:
           # eg: 2024-09-13T23:58:17.000Z / 2024-09-14T07:14:07.419698061Z
            if '.' in row[0]:
                timestamp = row[0][:26] + 'Z'  # Keep only microseconds, then re-add 'Z'
            else:
                timestamp = row[0]  # No adjustment needed

            # Remove 'Z' for parsing and interpret as UTC
            timestamp = timestamp.rstrip('Z')

            # Parse the timestamp
            parsed_time = datetime.strptime(timestamp, DATETIME_FORMAT)

            # Extract year, month, and date
            year = parsed_time.year
            month = parsed_time.month
            date = parsed_time.date()

            return {
                "date": date,  # Date
                "year": year,
                "month": month,
                "accession": row[3].split('/')[-2],  # Extract the PXD Accession from the path
                "filename": row[3].split('/')[-1],  # Extract the Filename from the path
                "completed": row[6].lower().strip(),  # Completion Status (e.g., Complete or Incomplete)
                "country": row[7],  # Country
                "method": row[11],  # Method (e.g., ftp, aspera)
            }
        except IndexError:
            return None
