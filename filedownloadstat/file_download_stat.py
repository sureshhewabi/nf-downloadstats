from stattypes.project_stat import ProjectStat
from stattypes.regional_stat import RegionalStat
from stattypes.trends_stat import TrendsStat
from stattypes.user_stat import UserStat
import pandas as pd


class FileDownloadStat:

    @staticmethod
    def run_file_download_stat(file, output):
        """
        Run the log file statistics generation and save the visualizations in an HTML output file.
        """

        # Load the Parquet file
        data = pd.read_json(file)
        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Add a `month_year` column
        df['month_year'] = df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2)

        # Ensure the x-axis displays all `month_year` values
        unique_month_years = sorted(df['month_year'].unique())

        # Total downloads per month-year
        total_downloads = df.groupby('month_year').size().reset_index(name='count')
        total_downloads['method'] = 'Total'  # Label for the total count

        # Downloads per month-year by method
        downloads_by_method = df.groupby(['month_year', 'method']).size().reset_index(name='count')

        # Combine total downloads and method-specific downloads
        combined_data = pd.concat([total_downloads, downloads_by_method])

        ProjectStat.combined_line_chart(combined_data, unique_month_years)

        ############ 2. Trends Statistics ############

        # Group data by year and month, and calculate the total count for each combination
        # monthly_data = df.groupby(['year', 'month']).size().reset_index(name='count')
        # TrendsStat.downloads_by_month(monthly_data)

        # Group data by date and count the occurrences
        # Group by date and method to sum the count of downloads per method per day
        # Group by 'date' and 'method' and count the occurrences of each combination
        daily_data = df.groupby(['date', 'method'], as_index=False).size().rename(columns={'size': 'count'})
        TrendsStat.download_over_treands(daily_data)

        ############ 3. Regional Statistics ############
        # Group data by country to get the count of downloads
        choropleth_data = df.groupby(['country', 'year']).size().reset_index(name='count')
        choropleth_data = choropleth_data.sort_values(by='year')
        RegionalStat.download_by_country(choropleth_data)

        ############ 4. User Statistics ############
        # Calculate unique users per date
        user_data = df.groupby(['date', 'year', 'month'], as_index=False)['user'].nunique()
        user_data['date'] = pd.to_datetime(user_data['date'], unit='ms')  # Convert date to datetime
        UserStat.unique_users_over_time(user_data)

        # Calculate unique users per country
        country_user_data = df.groupby(['country', 'year'], as_index=False)['user'].nunique()
        UserStat.users_by_country(country_user_data)


        # Group by user and country, and calculate counts
        active_users = df.groupby(["user", "country"]).size().reset_index(name="count")
        # Create a combined x-axis label
        active_users["user_country"] = active_users["user"] + " (" + active_users["country"] + ")"
        UserStat.top_ten_users(active_users)

        # Combine the HTML files
        with open(output, "w") as f:
            f.write("<h1>File Download Statistics</h1>")

            f.write("<h2> 1. Project level Statistics </h2>")
            # Embed the content of the generated HTML plots
            with open("combined_line_chart.html", "r") as combined_line_chart:
                f.write(combined_line_chart.read())

            f.write("<h2> 2. Trends Statistics </h2>")
            # with open("downloads_by_month.html", "r") as downloads_by_month:
            #     f.write(downloads_by_month.read())
            with open("download_over_treands.html", "r") as download_over_treands:
                f.write(download_over_treands.read())

            f.write("<h2> 3. Regional Statistics </h2>")
            with open("downloads_by_country.html", "r") as downloads_by_country:
                f.write(downloads_by_country.read())

            f.write("<h2> 4. Users Statistics </h2>")
            with open("unique_users_over_time.html", "r") as unique_users_over_time:
                f.write(unique_users_over_time.read())
            with open("users_by_country.html", "r") as users_by_country:
                f.write(users_by_country.read())
            with open("top_ten_users.html", "r") as top_ten_users:
                f.write(top_ten_users.read())



