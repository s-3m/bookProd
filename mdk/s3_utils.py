import asyncio
from aiobotocore.session import get_session
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import os

load_dotenv("../.env")


class S3Client:
    def __init__(self, access_key: str, secret_key: str, bucket_name: str):
        self.config = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "endpoint_url": "https://s3.ru-1.storage.selcloud.ru",
        }
        self.bucket = bucket_name
        self.session = get_session()

    @asynccontextmanager
    async def get_client(self):
        async with self.session.create_client("s3", **self.config) as client:
            yield client

    async def upload_file(self, file, name):
        async with self.get_client() as client:
            res = await client.put_object(Bucket=self.bucket, Key=name, Body=file)
            if res["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return (
                    f"https://88826fd1-7276-4186-85e7-3cfb551f1cb7.selstorage.ru/{name}"
                )
            return None


s3_client = S3Client(
    access_key=os.getenv("S3_ACCESS_KEY"),
    secret_key=os.getenv("S3_SECRET_KEY"),
    bucket_name=os.getenv("S3_BUCKET_NAME"),
)

#
# if __name__ == "__main__":
#     asyncio.run(s3_client.upload_file("1.png", "mdk_1212111111.png"))
