import numpy as np
from sklearn.cluster import DBSCAN
from pyspark.sql.functions import udf, col, explode, least, greatest, lit, concat, collect_list, struct, count
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, DoubleType, StringType


# --- Define UDF ---

def cluster_locs_with_precision(locs):
    if not locs:
        return []

    coords = []
    sources = []
    uuids = []

    for p in locs:
        if p['lat'] is not None and p['lon'] is not None:
            coords.append([p['lat'], p['lon']])
            sources.append(p['source'])  # Default to low if missing
            uuids.append(p['uuid'])  # Default to "n" if missing

    if len(coords) == 0:
        return []

    coords = np.array(coords)

    # DBSCAN clustering with 50 meters (0.05 km)
    kms_per_radian = 6371.0088
    epsilon = 0.05 / kms_per_radian
    db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine')
    db.fit(np.radians(coords))
    labels = db.labels_

    clusters = {}
    for label, point, source, uuid in zip(labels, coords, sources, uuids):
        clusters.setdefault(label, []).append((point, source, uuid))

    rep_points = []
    for points_with_source in clusters.values():
        # Separate coords and sources
        points = [pt for pt, _, _ in points_with_source]
        sources = [src for _, src, _ in points_with_source]
        uuids = [uuid for _, _, uuid in points_with_source]

        if 'high_precision' in sources:
            # Use first high-precision point
            for pt, src, uuid in points_with_source:
                if src == 'high_precision':
                    rep_points.append({
                        'lat': float(pt[0]),
                        'lon': float(pt[1]),
                        'source': 'high_precision',
                        'uuid': uuid
                    })
        elif 'low_precision' in sources:
            low_precision_points = [pt for pt, src, uuid in points_with_source if src == 'low_precision']

            if low_precision_points:  # Check if there are any low_precision_points to avoid error with np.mean
                # Use mean location of low_precision points
                mean_point = np.mean(low_precision_points, axis=0)
                rep_points.append({
                    'lat': float(mean_point[0]),
                    'lon': float(mean_point[1]),
                    'source': 'low_precision',
                    'uuid': max(uuids)
                })
        else:
            mean_point = np.mean(points, axis=0)
            rep_points.append({
                'lat': float(mean_point[0]),
                'lon': float(mean_point[1]),
                'source': 'low_precision',
                'uuid': max(uuids)
            })

    return rep_points


# --- Define Output Schema for UDF ---

rep_point_schema = ArrayType(
    StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("source", StringType(), True),
        StructField("uuid", StringType(), True)
    ])
)

cluster_locs_udf = udf(cluster_locs_with_precision, rep_point_schema)


def cluster_bumps(cluster_segments_df):
    return (
        cluster_segments_df
        .withColumn('new_u', F.least('forward_u', 'forward_v'))
        .withColumn('new_v', F.greatest('forward_u', 'forward_v'))
        .groupBy('cluster_id')
        .agg(
            F.collect_list(F.struct(
                F.col('matched_location_lat').alias('lat'),
                F.col('matched_location_lon').alias('lon'),
                F.col('source').alias('source'),
                F.col('new_u').alias('new_u'),
                F.col('new_v').alias('new_v'),
                F.col('uuid').alias('uuid'),
            ).alias('loc')).alias('locs'),
            F.count('*').alias('count')
        )
        .withColumn('bumps', cluster_locs_udf('locs'))
        .select(
            'cluster_id',
            'locs',
            F.explode('bumps').alias('bump'),
        )
        .select(
            'cluster_id',
            F.col('locs.new_u').alias('new_u'),
            F.col('locs.new_v').alias('new_v'),
            F.col('bump.lat').alias('lat'),
            F.col('bump.lon').alias('lon'),
            F.col('bump.uuid').alias('uuid'),
            col('bump.source').alias('source')
        )
    )
