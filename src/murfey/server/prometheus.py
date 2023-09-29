from prometheus_client import Gauge

seen_files = Gauge("acquired_files", "Number of files produced", ["rsync_source"])
transferred_files = Gauge(
    "transferred_files", "Number of files transferred", ["rsync_source"]
)
