import datetime
import os

from sparkJobs.speed_bump.config import OSM_NODES_PARQUET_PATH
from sparkJobs.speed_bump.jobs.speed_bump_pipeline import SpeedBumpPipeline
from sparkJobs.speed_bump.config import SPEED_BUMPS_DATA_PATH

osrm_address = "10.32.7.113:32222"


def run_speed_bump_job(spark_session, execution_date: datetime.datetime):
    write_data_path = os.path.join(SPEED_BUMPS_DATA_PATH, f'date={execution_date.date()}')
    speed_bump_app = SpeedBumpPipeline(execution_date, write_data_path, osrm_address, OSM_NODES_PARQUET_PATH, spark_session)
    speed_bump_app.execute()
