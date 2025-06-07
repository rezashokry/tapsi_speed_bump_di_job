from pyspark.sql import functions as F


def haversine(latitude1, longitude1, latitude2, longitude2, unit='M'):
    distance = F.acos(F.least(F.lit(1), F.greatest(F.lit(-1),
        F.sin(F.toRadians(latitude1)) * F.sin(F.toRadians(latitude2)) +
        F.cos(F.toRadians(latitude1)) * F.cos(F.toRadians(latitude2)) *
        F.cos(F.toRadians(longitude1) - F.toRadians(longitude2))
    ))) * F.lit(6371.0088)

    if unit == 'KM':
        return distance
    elif unit == 'M':
        return distance * 1000
    else:
        raise NotImplementedError()
