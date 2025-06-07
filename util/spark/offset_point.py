from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType
import math


# Helper function to compute destination point given start point, bearing, and distance
@F.udf(returnType=StructType([
    StructField("exact_lat", DoubleType()),
    StructField("exact_lon", DoubleType())
]))
def compute_offset_point(u_lat, u_lon, v_lat, v_lon, offset_m):
    if None in (u_lat, u_lon, v_lat, v_lon, offset_m):
        return None, None
    try:
        R = 6371000  # Earth's radius in meters
        φ1 = math.radians(u_lat)
        λ1 = math.radians(u_lon)
        φ2 = math.radians(v_lat)
        λ2 = math.radians(v_lon)

        Δλ = λ2 - λ1
        Δφ = φ2 - φ1

        # Initial bearing from u to v
        y = math.sin(Δλ) * math.cos(φ2)
        x = math.cos(φ1) * math.sin(φ2) - math.sin(φ1) * math.cos(φ2) * math.cos(Δλ)
        θ = math.atan2(y, x)  # Bearing in radians

        # Compute new lat/lon after moving `offset_m` from (u_lat, u_lon) along bearing θ
        δ = offset_m / R  # Angular distance

        φ_new = math.asin(math.sin(φ1) * math.cos(δ) + math.cos(φ1) * math.sin(δ) * math.cos(θ))
        λ_new = λ1 + math.atan2(
            math.sin(θ) * math.sin(δ) * math.cos(φ1),
            math.cos(δ) - math.sin(φ1) * math.sin(φ_new)
        )

        lat_new = math.degrees(φ_new)
        lon_new = math.degrees(λ_new)
        return float(lat_new), float(lon_new)
    except:
        return None, None
