# from dask.distributed import Client, progress
# from dask_jobqueue import SLURMCluster
# from dask.distributed import LocalCluster
# import panel as pn
#
#
# class DaskManager:
#     def __init__(self, profile="ebislurm", nodes=5, max_jobs=20):
#         """Initialize and manage a Dask cluster."""
#         if profile == "ebislurm":
#             ## Cluster configuration is defined in ~/.config/dask/jobqueue.yaml
#             self.cluster = SLURMCluster()
#             self.cluster.scale(nodes)
#             self.cluster.adapt(maximum_jobs=max_jobs)
#         else:
#             self.cluster = LocalCluster()
#
#         self.client = Client(self.cluster)
#         print(f"Dask Dashboard running at: {self.client.dashboard_link}")
#
#         # Optionally, save the dashboard report
#         pn.panel(self.client).save("dask_dashboard.html")
#
#         print(f"~~~~~~~~~~~~~~~~ D A S K    S T A R T E D ~~~~~~~~~~~~~~~~")
#         print(f"Dask cluster initialized: {self.cluster}")
#         print(f"Dask Client connected: {self.client}")
#
#     def close(self):
#         """Shutdown the Dask cluster and client properly."""
#         print("Shutting down Dask cluster...")
#         self.client.close()
#         self.cluster.close()
#         print(f"~~~~~~~~~~~~~~~~ D A S K    E N D ~~~~~~~~~~~~~~~~")
