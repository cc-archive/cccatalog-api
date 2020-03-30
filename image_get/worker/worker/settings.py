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

# Number of images to download and resize simultaneously.
BATCH_SIZE = 500
