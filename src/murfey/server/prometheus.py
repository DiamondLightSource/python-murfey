from prometheus_client import Counter, Gauge

seen_files = Gauge(
    "acquired_files", "Number of files produced", ["rsync_source", "visit"]
)
seen_data_files = Gauge(
    "acquired_data_files", "Number of data files produced", ["rsync_source", "visit"]
)
transferred_files = Gauge(
    "transferred_files", "Number of files transferred", ["rsync_source", "visit"]
)
transferred_data_files = Gauge(
    "transferred_data_files",
    "Number of data files transferred",
    ["rsync_source", "visit"],
)

transferred_files_bytes = Gauge(
    "transferred_files_bytes", "Size of files transferred", ["rsync_source", "visit"]
)
transferred_data_files_bytes = Gauge(
    "transferred_data_files_bytes",
    "Size of data files transferred",
    ["rsync_source", "visit"],
)

preprocessed_movies = Counter(
    "preprocessed_movies",
    "Number of movies that have been preprocessed",
    ["processing_job"],
)

exposure_time = Gauge("exposure_time", "Exposure time for a single movie")

monitoring_switch = Gauge(
    "monitoring_on",
    "Whether the corresponding visit should be monitored or not",
    ["visit"],
)
