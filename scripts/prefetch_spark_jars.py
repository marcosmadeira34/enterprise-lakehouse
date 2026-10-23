"""Download the Spark packages (Delta, hadoop-aws) into the Ivy cache at image build time.

Starts a throwaway local session configured for S3 so that both package sets are resolved; no S3
endpoint is contacted because nothing is read or written.
"""

from __future__ import annotations

import os

from lakehouse.common.spark import build_spark
from lakehouse.config import Settings


def main() -> None:
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "prefetch")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "prefetch")
    settings = Settings.load(
        env={
            **os.environ,
            "LAKEHOUSE__STORAGE__LAKE_URI": "s3://prefetch",
            "LAKEHOUSE__STORAGE__S3_ENDPOINT_URL": "http://localhost:9",
        }
    )
    spark = build_spark(settings, "prefetch")
    print("resolved jars:", spark.sparkContext.getConf().get("spark.jars.packages"))
    spark.stop()


if __name__ == "__main__":
    main()
