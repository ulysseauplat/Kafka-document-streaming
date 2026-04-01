import os

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "kafka-stream-project-ulysse")
S3_FLUSH_INTERVAL = int(os.getenv("S3_FLUSH_INTERVAL", "15"))
