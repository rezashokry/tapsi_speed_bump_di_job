import requests
import uuid
from time import sleep

from pyspark.sql import functions as F
from pyspark.sql import types as T
from disjoint_set import DisjointSet

from sparkJobs.speed_bump.util.spark.offset_point import compute_offset_point
from sparkJobs.speed_bump.util.spark.haversine import haversine
from sparkJobs.speed_bump.jobs.clustering import cluster_bumps


def clean_nodes_data(nodes_data):
    return (
        nodes_data
        .select(
            F.col('id').alias('node_id'),
            'latitude',
            'longitude',
        )
    )


def clean_cs_data(cs_data):
    return (
        cs_data
        .filter(F.col('type') == 'SPEED_BUMP')
        .select(
            F.col('engagement_location_time.latitude').alias('latitude'),
            F.col('engagement_location_time.longitude').alias('longitude'),
        )
        .withColumn('source', F.lit('new_cs'))
        .withColumn('uuid', F.lit('.'))
    )


def clean_production_bumps(production_bumps_data, node_df, confirmation_data):
    production_bump_locations_df = (
        production_bumps_data
        .alias('L')
        .join(node_df.alias('R'), on=F.col('u') == F.col('node_id'), how='inner')
        .select(
            'L.*',
            F.col('R.latitude').alias('u_lat'),
            F.col('R.longitude').alias('u_lon'),
        ).alias('L')
        .join(node_df.alias('R'), on=F.col('v') == F.col('node_id'), how='inner')
        .select(
            'L.*',
            F.col('R.latitude').alias('v_lat'),
            F.col('R.longitude').alias('v_lon'),
        )
        .withColumn("exact_location",
                    compute_offset_point(
                        "u_lat",
                        "u_lon",
                        "v_lat",
                        "v_lon",
                        F.col("uv_offset").cast('double')
                    )
                    )
        .withColumn("lat", F.col("exact_location.exact_lat"))
        .withColumn("lon", F.col("exact_location.exact_lon"))
        .drop("exact_location")
    )

    confirmation_df = (
        confirmation_data
        .filter(F.col('userResponse').isNull())
        .withColumn('confirm', F.when(F.col('confirmed') == 'true', 1).otherwise(-3))
        .groupBy(F.col('id').alias('uuid'))
        .agg(
            F.first('cluster_id').alias('cluster_id'),
            F.sum('confirm').alias('score'),
            F.count('*').alias('confirmation_count'),
        )
    )

    return (
        production_bump_locations_df
        .join(confirmation_df.select('uuid', 'score', 'confirmation_count'), on='uuid', how='left')
        .filter((F.col('score') > -9) | F.col('score').isNull())
        .select(
            'lat',
            'lon',
            'source',
            'uuid'
        )
    )


@F.udf(returnType=T.StructType([
        T.StructField("forward_u", T.LongType(), True),
        T.StructField("forward_v", T.LongType(), True),
        T.StructField("u", T.LongType(), True),
        T.StructField("v", T.LongType(), True),
        T.StructField("matched_location_lat", T.DoubleType(), True),
        T.StructField("matched_location_lon", T.DoubleType(), True),
    ])
)
def nearest_osrm(lat, lon, osrm_address):
    url = f"http://{osrm_address}/nearest/v1/driving/{lon},{lat}?include_alternatives=true"
    for _ in range(10):
        try:
            response = requests.get(url, headers={'Connection': 'close'})
            if response.status_code != 200:
                raise Exception(f'OSRM call failed, code: {response.status_code}. info: {response.text}')

            data = response.json()
            if "waypoints" in data and len(data["waypoints"]) > 0:
                nearest = data["waypoints"][0]
                return {
                    'forward_u': nearest['forward_u'],
                    'forward_v': nearest['forward_v'],
                    'u': nearest['nodes'][0],
                    'v': nearest['nodes'][1],
                    'matched_location_lat': nearest['location'][1],
                    'matched_location_lon': nearest['location'][0],
                }
            else:
                return None
        except Exception as e:
            print(f"error: {str(e)}")
            sleep(2)


def cluster_segments(segments_df, spark_session):
    output = []
    disjoint_set = DisjointSet()
    segments_pdf = (
        segments_df
        .select(
            F.col('forward_u').alias('fromNode'),
            F.col('forward_v').alias('toNode'),
        )
    ).toPandas()

    for fromNode, toNode in segments_pdf[['fromNode', 'toNode']].itertuples(index=False):
        disjoint_set.union(fromNode, toNode)

    for fromNode, toNode in segments_pdf[['fromNode', 'toNode']].itertuples(index=False):
        output.append([fromNode, toNode, disjoint_set.find(fromNode)])

    return (
        spark_session.createDataFrame(data=output, schema=['forward_u', 'forward_v', 'cluster_id'])
        .join(segments_df, on=['forward_u', 'forward_v'], how='inner')
    )


@F.udf(returnType=T.StringType())
def generate_uuid():
    return str(uuid.uuid4())


def add_coordinates_to_nodes(node_df, df):
    return (
        df.alias('L')
        .join(node_df.alias('R'), on=F.col('u') == F.col('node_id'), how='inner')
        .select(
            'L.*',
            F.col('R.latitude').alias('u_lat'),
            F.col('R.longitude').alias('u_lon'),
        ).alias('L')
        .join(node_df.alias('R'), on=F.col('v') == F.col('node_id'), how='inner')
        .select(
            'L.*',
            F.col('R.latitude').alias('v_lat'),
            F.col('R.longitude').alias('v_lon'),
        )
    )


class SpeedBumpAnnotator():
    def __init__(self, osrm_address, spark_session):
        self.osrm_address = osrm_address
        self.spark_session = spark_session

    def execute(self, node_data, production_bumps_data, cs_data, confirmation_data):
        node_df = clean_nodes_data(node_data)
        cs_df = clean_cs_data(cs_data)
        production_bumps_data = clean_production_bumps(production_bumps_data, node_df, confirmation_data)

        nearest_df = (
            production_bumps_data
            .union(cs_df)
            .withColumn('nearest', nearest_osrm('lat', 'lon', F.lit(self.osrm_address)))
            .select(
                'lat',
                'lon',
                'nearest.*',
                'source',
                'uuid',
            )
        )

        bumps_with_segment_cluster_id_df = cluster_segments(nearest_df, self.spark_session)
        unique_bumps_df = cluster_bumps(bumps_with_segment_cluster_id_df)
        unique_bumps_new_nearest_df = (
            unique_bumps_df
            .withColumn('new_nearest', nearest_osrm('lat', 'lon', F.lit(self.osrm_address)))
            .withColumn('uuid', F.when(F.col('uuid') == '.', generate_uuid()).otherwise(F.col('uuid')))
            .select(
                'new_nearest.*',
                'cluster_id',
                'uuid',
                'source'
            )
        )

        unique_bumps_with_coordiantes_df = add_coordinates_to_nodes(node_df, unique_bumps_new_nearest_df)

        return (
            unique_bumps_with_coordiantes_df
            .distinct()
            .select(
                'u',
                'v',
                haversine('u_lat', 'u_lon', 'matched_location_lat', 'matched_location_lon').alias('uv_offset'),
                haversine('v_lat', 'v_lon', 'matched_location_lat', 'matched_location_lon').alias('vu_offset'),
                'matched_location_lat', 'matched_location_lon',
                'source',
                'uuid',
                'cluster_id',
                F.lit('SPEED_BUMP').alias('type'),
                F.lit(0.0).alias('confidence'),
                F.lit(True).alias('confirmation'),
                F.lit(0).alias('contributions'),
            )
        )
