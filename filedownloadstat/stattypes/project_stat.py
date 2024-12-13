import plotly.express as px


class ProjectStat:
    """
    Project Level stat
    """

    @staticmethod
    def downloads_per_accession(accession_data):
        """
        Highlight popular projects based on their accession
        """
        # Create the bar chart
        fig = px.bar(
            accession_data,
            x="accession",  # X-axis: Project accession
            y="count",  # Y-axis: Count of downloads
            title="Downloads Per Project Accession",  # Chart title
            labels={"accession": "Project Accession", "count": "Downloads"}  # Axis labels
        )
        fig.write_html('downloads_per_accession.html')
