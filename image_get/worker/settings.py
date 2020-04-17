import os

# S3
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')

# Datastores
KAFKA_HOSTS = os.getenv('KAFKA_HOSTS', 'kafka:9092')
ZOOKEEPER_HOST = os.getenv('ZOOKEEPER_HOST', 'zookeeper:2181')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')

# Thumbnail size
TARGET_RESOLUTION = (640, 480)

# Number of tasks to schedule (but not necessarily execute) simultaneously.
# Each pending resize task takes ~3kb of memory, so scheduling 1MM events means
# consuming 3gb of memory in pending tasks alone.
SCHEDULE_SIZE = int(os.getenv('SCHEDULE_SIZE', '3000'))

# Number of resize tasks to run concurrently.
# Each task requires a connection to Redis, so be mindful of the max connection
# limit. Also be mindful that running too many coroutines simultaneously can
# be detrimental to performance.
MAX_TASKS = 3000
