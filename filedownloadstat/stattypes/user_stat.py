import plotly.express as px


class UserStat:
    """
    User Level stat
    """

    @staticmethod
    def unique_users_over_time(user_data):
        fig = px.line(
            user_data,
            x='date',
            y='user',
            title='Unique Users Over Time',
            labels={"date": "Date", "user": "Unique Users"}
        )
        fig.write_html("unique_users_over_time.html")


    @staticmethod
    def users_by_country(country_user_data):
        """
        Plot users by country with animation for each year
        """
        fig = px.scatter_geo(
            country_user_data,
            locations='country',
            locationmode='country names',
            size='user',
            color='user',
            hover_name='country',
            animation_frame='year',
            title='Unique Users by Country Over Time',
            labels={"user": "Unique Users", "country": "Country"},
            size_max=50,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig.write_html("users_by_country.html")

