from sparkJobs.speed_bump.jobs import speed_bump_annotator
from sparkJobs.speed_bump.util.redis_handler import RedisHandler
from sparkJobs.speed_bump.config import SPEED_BUMP_CONFIRMATION_START_DATE
from util.passport_dataloader_api import load_stream_data


class SpeedBumpPipeline():
    def __init__(self, date, osrm_address, write_data_path, osm_parquets_nodes_filename, spark_session):
        self.osm_parquets_nodes_filename = osm_parquets_nodes_filename
        self.spark_session = spark_session
        self.write_data_path = write_data_path
        self.date = date

        self.speed_bump_annotator = speed_bump_annotator.SpeedBumpAnnotator(osrm_address, spark_session)
        self.redis_handler = RedisHandler()

    def load_data(self):
        return_dict = {
            "node_data": self.spark_session.read.parquet(self.osm_parquets_nodes_filename),
            "production_bumps_data": self.spark_session.createDataFrame(self.redis_handler.read_data_from_redis()),
            "crowd_source_reports_data": load_stream_data(
                spark=self.spark_session,
                table='crowd_source_reports',
                columns=[
                    'type',
                    'engagement_location_time',
                ],
                from_date=self.date,
                to_date=self.date
            ),
            "confirmation_data": load_stream_data(
                spark=self.spark_session,
                table='crowd_sourcing_speed_bump_confirmation',
                columns=[
                    'userResponse',
                    'confirmed',
                    'id',
                    'cluster_id',
                    'source',
                    'u',
                    'v',
                ],
                from_date=SPEED_BUMP_CONFIRMATION_START_DATE,  #todo: aggregation job
                to_date=self.date
            )
        }

        return return_dict

    def save_segment_features(self, bumps_df):
        bumps_df.write.format("parquet").save(self.write_data_path)

    def execute(self):
        input_data_dict = self.load_data()
        bumps_df = self.speed_bump_annotator.execute(
            node_data=input_data_dict['node_data'],
            production_bumps_data=input_data_dict['production_bumps_data'],
            cs_data=input_data_dict['crowd_source_reports_data'],
            confirmation_data=input_data_dict['confirmation_data'],
        )
        self.redis_handler.write_df_data_to_redis(bumps_df)
        self.save_segment_features(bumps_df)
        # self.save_segment_features(segment_features_df)
