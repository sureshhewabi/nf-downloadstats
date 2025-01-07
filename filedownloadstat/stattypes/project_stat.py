import plotly.express as px


class ProjectStat:
    """
    Project Level stat
    """

    @staticmethod
    def combined_line_chart(combined_data, unique_month_years):
        """
        Highlight number of monthly downloads
        """
        # Plot: Combined line chart
        fig = px.line(
            combined_data,
            x='month_year',
            y='count',
            color='method',
            title='Monthly Downloads: Total and by Method',
            labels={'month_year': 'Month-Year', 'count': 'Downloads', 'method': 'Download Method'}
        )
        # Update layout for fixed x-axis ticks
        fig.update_layout(
            xaxis=dict(
                tickmode='array',
                tickvals=unique_month_years,
                ticktext=unique_month_years
            ),
            xaxis_tickangle=-45
        )
        # Update layout for smoother animation
        # fig.update_layout(transition={'duration': 500}, xaxis_tickangle=-45)

        fig.write_html('combined_line_chart.html')

    @staticmethod
    def yearly_download(yearly_downloads):
        """
        Create a bar chart with year on X-axis, count on Y-axis, and method as color
        """
        fig = px.bar(
            yearly_downloads,
            x="year",
            y="count",
            color="method",
            barmode="group",  # Group bars by method
            title="Yearly Total Downloads Separated by Method",
            labels={"count": "Downloads", "year": "Year", "method": "Download Method"}
        )
        fig.write_html('yearly_download.html')

    @staticmethod
    def cumulative_download(monthly_downloads):
        # Create line chart
        fig = px.line(
            monthly_downloads,
            x="month_year",
            y="cumulative_count",
            title="Cumulative Downloads Over Time (Month-Year)",
            labels={"month_year": "Month-Year", "cumulative_count": "Cumulative Downloads"},
            markers=True  # Adds markers to show data points clearly
        )
        fig.write_html('cumulative_download.html')

    @staticmethod
    def project_downloads_histogram_1(download_counts):
        """
        This will create a histogram where each bar represents how many projects fall into different download count ranges
        """
        fig = px.histogram(
            download_counts,
            x="download_count",
            nbins=100,  # Set the number of bins
            title="Distribution of Projects by Download Count",
            labels={"download_count": "Number of Downloads", "count": "Number of Projects"},
        )
        fig.write_html('project_downloads_histogram_1.html')

    @staticmethod
    def project_downloads_histogram_2(download_counts):
        """
        This will create a histogram where each bar represents how many projects fall into different download count ranges
        """
        fig = px.histogram(
            download_counts,
            x="download_count",
            nbins=100,  # Set the number of bins
            title="Distribution of Projects by Download Count",
            labels={"download_count": "Number of Downloads", "count": "Number of Projects"},
        )
        fig.write_html('project_downloads_histogram_2.html')