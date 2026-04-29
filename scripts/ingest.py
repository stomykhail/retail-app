import argparse
import logging
import os
import sys

# Add the project root to the Python path to resolve 'src' module imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import boto3
from boto3.s3.transfer import TransferConfig

from src.config import settings


logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload raw retail CSV directly to S3.")
    parser.add_argument(
        "--file",
        default=settings.RAW_DATA_PATH,
        help="Path to local source CSV file.",
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("RAW_BUCKET"),
        help="Destination S3 bucket name. Defaults to env var RAW_BUCKET.",
    )
    parser.add_argument(
        "--key",
        default="raw/source_1150208_to_1171119_part.csv",
        help="Destination S3 object key.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    if not args.bucket:
        raise SystemExit("Missing S3 bucket. Pass --bucket or set RAW_BUCKET env var.")

    logger.info(f"Uploading raw CSV directly to S3... bucket={args.bucket} key={args.key}")
    s3 = boto3.client("s3")
    transfer_config = TransferConfig(max_concurrency=4)
    s3.upload_file(args.file, args.bucket, args.key, Config=transfer_config)
    logger.info("Upload complete!")


if __name__ == "__main__":
    main()
