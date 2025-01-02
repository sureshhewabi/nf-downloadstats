import plotly.express as px


class TrendsStat:

    @staticmethod
    def download_over_treands(daily_data):
        """
        Understand download trends over days, months, or years.
        """
        # Create the line chart with 'date' on the x-axis and 'count' on the y-axis
        fig = px.line(
            daily_data,
            x='date',  # X-axis: Date
            y='count',  # Y-axis: Count of downloads
            color='method',  # Group by 'method' to create separate lines
            title='File Downloads Over Time',  # Chart title
            labels={"date": "Date", "count": "Downloads", "method": "Method"}  # Axis labels
        )
        fig.write_html("download_over_treands.html")

    # @staticmethod
    # def downloads_by_month(monthly_data):
    #     """
    #     Aggregate downloads by year or month.
    #     """
    #     # Create the bar chart grouped by month and year
    #     fig = px.bar(
    #         monthly_data,
    #         x='month',  # X-axis: Month
    #         y='count',  # Y-axis: Count of downloads
    #         color='year',  # Color bars by year
    #         barmode='group',  # Group bars by year
    #         title='Monthly Downloads Grouped by Year',  # Chart title
    #         labels={"month": "Month", "count": "Downloads", "year": "Year"}  # Axis labels
    #     )
    #     fig.write_html("downloads_by_month.html")