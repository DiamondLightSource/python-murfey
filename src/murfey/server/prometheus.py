from prometheus_client import Counter, Gauge

seen_files = Gauge(
    "acquired_files",
    "Number of files produced",
    ["microscope", "rsync_source", "visit"],
)
seen_data_files = Gauge(
    "acquired_data_files",
    "Number of data files produced",
    ["microscope", "rsync_source", "visit"],
)
transferred_files = Gauge(
    "transferred_files",
    "Number of files transferred",
    ["microscope", "rsync_source", "visit"],
)
transferred_data_files = Gauge(
    "transferred_data_files",
    "Number of data files transferred",
    ["microscope", "rsync_source", "visit"],
)

transferred_files_bytes = Gauge(
    "transferred_files_bytes",
    "Size of files transferred",
    ["microscope", "rsync_source", "visit"],
)
transferred_data_files_bytes = Gauge(
    "transferred_data_files_bytes",
    "Size of data files transferred",
    ["microscope", "rsync_source", "visit"],
)

skipped_files = Gauge(
    "skipped_files",
    "Number of files not transferred due to end time",
    ["microscope", "rsync_source", "visit"],
)

preprocessed_movies = Counter(
    "preprocessed_movies",
    "Number of movies that have been preprocessed",
    ["microscope", "processing_job"],
)

exposure_time = Gauge(
    "exposure_time", "Exposure time for a single movie", ["microscope", "visit"]
)


alert_end_time = Gauge("alert_end_time", "End time for alerts", ["microscope", "visit"])
