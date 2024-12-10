import plotly.express as px


class FileDownloadStat:

    @staticmethod
    def run_file_download_stat(file, output):
        """
        Run the log file statistics generation and save the visualizations in an HTML output file.
        """
        # Generate the visualizations
        # TODO: call visualisation methods

        # Combine the HTML files
        with open(output, "w") as f:
            f.write("<h1>File Download Statistics</h1>")

            # Embed the content of the generated HTML plots
            with open("file_download_stat.html", "r") as dist_file:
                f.write(dist_file.read())
