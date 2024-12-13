import plotly.express as px


class RegionalStat:

    @staticmethod
    def download_by_country(data):
        """
         Visualize downloads geographically
        """
        fig = px.choropleth(data,
                            locations='country',
                            locationmode='country names',
                            color='count',
                            title='Downloads by Country',
                            labels={"count": "Downloads"}
        )
        fig.write_html("downloads_by_country.html")
