from prometheus_client import Counter, Gauge

seen_files = Gauge("acquired_files", "Number of files produced", ["rsync_source"])
seen_data_files = Gauge(
    "acquired_data_files", "Number of data files produced", ["rsync_source"]
)
transferred_files = Gauge(
    "transferred_files", "Number of files transferred", ["rsync_source"]
)
transferred_data_files = Gauge(
    "transferred_data_files", "Number of data files transferred", ["rsync_source"]
)

transferred_files_bytes = Gauge(
    "transferred_files_bytes", "Size of files transferred", ["rsync_source"]
)
transferred_data_files_bytes = Gauge(
    "transferred_data_files_bytes", "Size of data files transferred", ["rsync_source"]
)

preprocessed_movies = Counter(
    "preprocessed_movies",
    "Number of movies that have been preprocessed",
    ["processing_job"],
)

exposure_time = Gauge("exposure_time", "Exposure time for a single movie")
