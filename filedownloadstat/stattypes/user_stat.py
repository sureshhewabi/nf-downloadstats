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
            size='user',  # Bubble size
            hover_name='country',
            animation_frame='year',
            title='Unique Users by Country Over Time',
            labels={"user": "Unique Users", "country": "Country"},
            size_max=50
        )
        fig.write_html("users_by_country.html")


    @staticmethod
    def top_ten_users(active_users):
        """
        If you want to visualize user-level details, such as the most active users or their activities over time
        """
        # Plot the top active users
        fig = px.bar(
            active_users,
            x="user_country",  # Combined user and country as x-axis
            y="count",  # Count as y-axis
            title="Custom User Behavior Analysis",
            labels={"count": "Downloads", "user_country": "User (Country)"}
        )
        fig.write_html("top_ten_users.html")