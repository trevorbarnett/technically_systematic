import time
import json
import logging
from io import StringIO
import sys
import pathlib
from lib.config import LoggingLevel

class OutputManifest:
  def __init__(self, pipeline_name: str,manifest_dir: str, logging_level: LoggingLevel = LoggingLevel.INFO):
    self.pipeline_name = pipeline_name
    self.manifest_dir = manifest_dir
    self.start_time = time.time()
    self.logs = []
    self.cache_files = []
    self.status = "In Progress"
    self.job_timings = {}
    self.events = []  # To track job events
    self.total_duration = None
    self.logging_level = logging_level
    self.dask_telemtry = None

    pathlib.Path(self.manifest_dir).mkdir(exist_ok=True,parents=True)
    self._output_filename = self._output_path()

    # Set up logging
    self._setup_logging()

  def _setup_logging(self):
    """
    Set up logging to capture stdout and stderr.
    """
    self.log_stream = StringIO()
    log_handler = logging.StreamHandler(self.log_stream)
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(log_formatter)

    # Set up root logger
    self.logger = logging.getLogger(self.pipeline_name)
    self.logger.setLevel(getattr(logging,self.logging_level.value))
    self.logger.addHandler(log_handler)
    self.logger.propagate = False

    # Redirect stdout and stderr
    sys.stdout = self._StreamToLogger(self.logger, logging.INFO)
    sys.stderr = self._StreamToLogger(self.logger, logging.ERROR)

  class _StreamToLogger:
    """
    Redirect stdout or stderr to the logger.
    """
    def __init__(self, logger, level):
      self.logger = logger
      self.level = level

    def write(self, message):
      if message.strip():
        self.logger.log(self.level, message)

    def flush(self):
      pass

  def add_log(self, message: str, level: LoggingLevel = LoggingLevel.INFO, exception : Exception = None):
    """
    Add a log message manually.
    """
    level = getattr(logging,level.value)
    self.logger.log(level,message,exception)

  def add_cache_file(self, file_path: str):
    """
    Track a generated cache file.
    """
    self.cache_files.append(file_path)

  def start_job(self, job_name: str):
    """
    Log the start of a job.
    """
    self.events.append({
      "timestamp": time.time(),
      "job_name": job_name,
      "event": "start_job"
    })

    self.job_timings[job_name] = {
      "start_time": time.time()
    }

  def trigger_event(self, job_name: str, event_name: str):
    """
    Log an intermediate event during a job.
    """
    self.events.append({
      "timestamp": time.time(),
      "job_name": job_name,
      "event": event_name
    })

  def end_job(self, job_name: str, success: bool):
    """
    Log the end of a job.
    """
    end_time = time.time()
    self.events.append({
      "timestamp": end_time,
      "job_name": job_name,
      "event": "end_job",
      "success": success
    })

    # Track job timing
    if job_name in self.job_timings:
      self.job_timings[job_name]["end_time"] = end_time
      self.job_timings[job_name]["duration"] = end_time - self.job_timings[job_name]["start_time"]
    else:
      self.job_timings[job_name] = {
        "end_time": end_time,
        "duration": None
      }

  def finalize(self, status: str):
    """
    Finalize the manifest with pipeline status and timing.
    """
    self.status = status
    self.end_time = time.time()
    self.total_duration = self.end_time - self.start_time

    # Include logs in the manifest
    self.logs = self.log_stream.getvalue().splitlines()
  def _output_path(self) -> str:
    out_dir = pathlib.Path(self.manifest_dir)
    out_file = pathlib.Path(f"{self.pipeline_name}-{time.time()}.json")
    return out_dir / out_file
  def save(self):
    """
    Save the manifest to a JSON file.
    """
    file_path =self._output_filename
    manifest_data = {
      "pipeline_name": self.pipeline_name,
      "status": self.status,
      "logs": self.logs,
      "cache_files": self.cache_files,
      "events": self.events,
      "job_timings": self.job_timings,
      "total_duration": self.total_duration,
    }
    with open(file_path, "w") as f:
      json.dump(manifest_data, f, indent=4)
    print(f"Manifest saved to {file_path}")
