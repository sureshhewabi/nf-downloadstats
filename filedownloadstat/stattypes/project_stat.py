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
        fig = px.line(
            download_counts,
            x="download_count",
            y="num_projects",
            title="Distribution of Projects by Download Count",
            labels={"download_count": "Number of Downloads", "num_projects": "Number of Projects"},
            markers=True  # Add markers to highlight points
        )
        fig.write_html('project_downloads_histogram_1.html')

    @staticmethod
    def top_downloaded_projects(df):
        """
        This will create a horizontal bar chart with Top 10 Most Downloaded Projects
        """
        # fig = px.bar(
        #     top_10_projects,
        #     x="download_count",
        #     y="accession_url",  # Use the clickable version
        #     orientation="h",  # Horizontal bar chart
        #     title="Top " + str(number_of_projects) + " Most Downloaded Projects",
        #     labels={"download_count": "Number of Downloads", "accession_url": "Project Accession"},
        #     text="download_count",  # Display counts on bars
        # )
        df["accession_url"] = df["accession"].apply(
            lambda x: f'<a href="https://example.com/{x}" target="_blank">{x}</a>')

        top_n_options = [5, 10, 15]

        # Create initial figure (default to Top 10)
        initial_top_n = 10
        filtered_df = df.nlargest(initial_top_n, "download_count")

        fig = px.bar(
            filtered_df,
            x="download_count",
            y="accession_url",  # Use the clickable version
            orientation="h",
            title="Top Downloaded Projects",
            labels={"download_count": "Number of Downloads", "accession_url": "Project Accession"},
            text="download_count"
        )

        # Add dropdown menu for selecting Top N projects
        dropdown_buttons = [
            {
                "label": f"Top {n}",
                "method": "update",
                "args": [
                    {"x": [df.nlargest(n, "download_count")["download_count"]],
                     "y": [df.nlargest(n, "download_count")["accession_url"]],
                     "text": [df.nlargest(n, "download_count")["download_count"]]
                     },
                    {"title": f"Top {n} Downloaded Projects"}
                ]
            }
            for n in top_n_options
        ]

        fig.update_layout(
            updatemenus=[{
                "buttons": dropdown_buttons,
                "direction": "down",
                "showactive": True,
            }]
        )

        fig.write_html('top_downloaded_projects.html')
