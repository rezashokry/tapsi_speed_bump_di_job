import redis
import json
# Define Sentinel connection parameters
sentinel_hosts = [
    ('redis-sentinel-able.tap30.server', 28100),
    ('redis-sentinel-baker.tap30.server', 28100),
    ('redis-sentinel-charlie.tap30.server', 28100)
]
REDIS_MASTER_NAME = "annotator"
LATEST_VERSION_POINTER_KEY = "speed_bumps_latest_version"
KEY_TO_READ = "speed_bumps_v"


class RedisHandler:
    def __init__(self, use_local_redis=False):
        if use_local_redis:
            # Just for test
            self.redis = redis.Redis(host='localhost', port=6379,
                                     decode_responses=True)
        else:
            # Create a connection to the Sentinel
            sentinel = redis.Redis(
                host=sentinel_hosts[2][0],
                port=sentinel_hosts[2][1],
                decode_responses=True  # To receive string instead of bytes
            )
            master_address = sentinel.sentinel_master(REDIS_MASTER_NAME)

            print(master_address)
            # Connect to the Redis master using the obtained address
            self.redis = redis.Redis(
                host=master_address["ip"],
                port=master_address["port"],
                decode_responses=True
            )
        self.version = self.redis.get(LATEST_VERSION_POINTER_KEY)

    def record_set_key(self, version):
        return f"{KEY_TO_READ}{version}"

    @property
    def version_key(self):
        return LATEST_VERSION_POINTER_KEY

    def update_version(self, version):
        self.redis.set(self.version_key, str(version))

    def expire_old_content(self):
        keys_to_keep = [
            self.version_key,
            self.record_set_key(self.version),
            self.record_set_key(str(int(self.version) + 1)),
        ]
        all_keys = self.redis.keys(f'{KEY_TO_READ}*')
        for key in all_keys:
            if key not in keys_to_keep:
                # Note that we should not remove them immediately because
                # a reader may be in the middle of reading the old version.
                self.redis.expire(key, 7 * 24 * 60 * 60)

    def write_df_data_to_redis(self, data_df):
        print("Collecting data on driver to write it to redis ")
        for row in data_df.collect():
            row = row.asDict(recursive=True)
            key = row["uuid"]
            value = row
            json_str = json.dumps(value)
            self.redis.hset(self.record_set_key(int(self.version) + 1), key, json_str)
        self.update_version(int(self.version) + 1)
        self.expire_old_content()

    def read_data_from_redis(self):
        result = []
        latest_version_date_str = self.redis.get(self.version_key)
        if latest_version_date_str:
            print(f"Latest version date found: {self.version}")
            key_to_read = f"speed_bumps_v{self.version}"
            print(f"Attempting to read data from key: {key_to_read}")

            key_list = self.redis.hkeys(key_to_read)
            for key in key_list:
                result.append(json.loads(self.redis.hget(key_to_read, key)))
                return result
        else:
            print(f"No version pointer found at '{LATEST_VERSION_POINTER_KEY}'.")
