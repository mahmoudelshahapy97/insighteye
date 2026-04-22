import boto3
import base64
import uuid
import logging
from botocore.exceptions import ClientError
from app.config.settings import config
import asyncio

logger = logging.getLogger(__name__)

class S3Service:
    def __init__(self):
        if not config.s3_bucket_name:
            logger.warning("S3_BUCKET_NAME is not set. S3 service will not work properly.")
            self.s3_client = None
        else:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=config.aws_access_key_id,
                aws_secret_access_key=config.aws_secret_access_key,
                region_name=config.aws_region
            )

    async def upload_image_base64_to_s3(self, base64_image: str, filename: str = None) -> str:
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return None

        if not filename:
            filename = f"images/{uuid.uuid4()}.jpg"

        try:
            image_data = base64.b64decode(base64_image)
            
            def upload():
                self.s3_client.put_object(
                    Bucket=config.s3_bucket_name,
                    Key=filename,
                    Body=image_data,
                    ContentType='image/jpeg'
                )
                return f"s3://{config.s3_bucket_name}/{filename}"

            s3_path = await asyncio.to_thread(upload)
            return s3_path
        except Exception as e:
            logger.error(f"Failed to upload image to S3: {e}")
            return None
            
    async def upload_video_file_to_s3(self, file_path: str, filename: str = None) -> str:
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return None
            
        if not os.path.exists(file_path):
            return None

        if not filename:
            filename = f"videos/{uuid.uuid4()}.mp4"

        try:
            def upload():
                self.s3_client.upload_file(
                    Filename=file_path,
                    Bucket=config.s3_bucket_name,
                    Key=filename,
                    ExtraArgs={'ContentType': 'video/mp4'}
                )
                return f"s3://{config.s3_bucket_name}/{filename}"

            s3_path = await asyncio.to_thread(upload)
            return s3_path
        except Exception as e:
            logger.error(f"Failed to upload video to S3: {e}")
            return None
            
    async def get_presigned_url(self, s3_uri: str, expiration=3600) -> str:
        if not self.s3_client or not s3_uri.startswith('s3://'):
            return s3_uri
            
        try:
            parts = s3_uri.replace('s3://', '').split('/', 1)
            if len(parts) != 2:
                return s3_uri
            bucket, key = parts
                
            def generate():
                return self.s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket, 'Key': key},
                    ExpiresIn=expiration
                )
            
            return await asyncio.to_thread(generate)
        except Exception as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            return s3_uri

    async def get_image_data(self, s3_uri: str) -> bytes:
        if not self.s3_client or not s3_uri.startswith('s3://'):
            return None
        try:
            parts = s3_uri.replace('s3://', '').split('/', 1)
            bucket, key = parts
            def fetch():
                response = self.s3_client.get_object(Bucket=bucket, Key=key)
                return response['Body'].read()
            return await asyncio.to_thread(fetch)
        except Exception as e:
            logger.error(f"Failed to fetch image from S3: {e}")
            return None

s3_service = S3Service()
