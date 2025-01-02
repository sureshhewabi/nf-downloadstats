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
