from stattypes.project_stat import ProjectStat
from stattypes.regional_stat import RegionalStat
from stattypes.trends_stat import TrendsStat
import pandas as pd


class FileDownloadStat:

    @staticmethod
    def run_file_download_stat(file, output):
        """
        Run the log file statistics generation and save the visualizations in an HTML output file.
        """

        # Load the Parquet file
        df = pd.read_json(file)
        df['date'] = pd.to_datetime(df['date'], unit='ms')

        ############ 1. Project level Statistics ############
        # Group data by accession and calculate the total count for each accession
        accession_data = df.groupby("accession").size().reset_index(name="count")

        # Sort data by count for better visualization (optional)
        accession_data = accession_data.sort_values(by="count", ascending=False)

        ProjectStat.downloads_per_accession(accession_data)

        ############ 2. Trends Statistics ############

        # Group data by year and month, and calculate the total count for each combination
        monthly_data = df.groupby(['year', 'month']).size().reset_index(name='count')
        TrendsStat.downloads_by_month(monthly_data)

        # Group data by date and count the occurrences
        # Group by date and method to sum the count of downloads per method per day
        # Group by 'date' and 'method' and count the occurrences of each combination
        daily_data = df.groupby(['date', 'method'], as_index=False).size().rename(columns={'size': 'count'})
        TrendsStat.download_over_treands(daily_data)

        ############ 3. Regional Statistics ############
        # Group data by country to get the count of downloads
        choropleth_data = df.groupby("country").size().reset_index(name="count")
        # print(choropleth_data)
        RegionalStat.download_by_country(choropleth_data)

        # Combine the HTML files
        with open(output, "w") as f:
            f.write("<h1>File Download Statistics</h1>")

            f.write("<h2> 1. Project level Statistics </h2>")
            # Embed the content of the generated HTML plots
            with open("downloads_per_accession.html", "r") as downloads_per_accession:
                f.write(downloads_per_accession.read())

            f.write("<h2> 2. Trends Statistics </h2>")
            with open("downloads_by_month.html", "r") as downloads_by_month:
                f.write(downloads_by_month.read())
            with open("download_over_treands.html", "r") as download_over_treands:
                f.write(download_over_treands.read())

            f.write("<h2> 3. Regional Statistics </h2>")
            with open("downloads_by_country.html", "r") as downloads_by_country:
                f.write(downloads_by_country.read())
