"""Object storage access for the ingestion layer (no Spark): local filesystem or any S3-compatible endpoint.

URIs are either absolute local paths or ``s3://bucket/key``. Spark needs the ``s3a://`` scheme, which
:func:`to_spark_path` provides; nothing else in the code base should build Hadoop-specific paths.
"""

from __future__ import annotations

import hashlib
import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from lakehouse.config import StorageSettings

CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True)
class StoredFile:
    uri: str
    size_bytes: int
    modified_at: datetime

    @property
    def name(self) -> str:
        return self.uri.rstrip("/").rsplit("/", 1)[-1]


class ObjectStore(Protocol):
    def list_files(self, prefix_uri: str) -> list[StoredFile]: ...

    def iter_chunks(self, uri: str) -> Iterator[bytes]: ...

    def read_bytes(self, uri: str) -> bytes: ...

    def put_bytes(self, uri: str, data: bytes) -> None: ...

    def exists(self, uri: str) -> bool: ...

    def delete(self, uri: str) -> None: ...

    def ensure_container(self, uri: str) -> None: ...


def sha256_of(store: ObjectStore, uri: str) -> str:
    """Content hash, streamed so large files never sit in memory."""
    digest = hashlib.sha256()
    for chunk in store.iter_chunks(uri):
        digest.update(chunk)
    return digest.hexdigest()


def join_uri(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *(p.strip("/") for p in parts)])


def is_s3(uri: str) -> bool:
    return uri.startswith(("s3://", "s3a://"))


def to_spark_path(uri: str) -> str:
    if uri.startswith("s3://"):
        return "s3a://" + uri[len("s3://") :]
    return uri


def from_spark_path(path: str) -> str:
    if path.startswith("s3a://"):
        return "s3://" + path[len("s3a://") :]
    if path.startswith("file:"):
        return "/" + path[len("file:") :].lstrip("/")
    return path


class LocalStore:
    def list_files(self, prefix_uri: str) -> list[StoredFile]:
        root = Path(prefix_uri)
        if not root.exists():
            return []
        files = []
        for path in sorted(root.rglob("*")):
            # Hidden files and directories are writer scratch space (e.g. temporary uploads), not data.
            if path.is_file() and not any(
                part.startswith((".", "_")) for part in path.relative_to(root).parts
            ):
                stat = path.stat()
                files.append(StoredFile(str(path), stat.st_size, datetime.fromtimestamp(stat.st_mtime, UTC)))
        return files

    def iter_chunks(self, uri: str) -> Iterator[bytes]:
        with open(uri, "rb") as handle:
            while chunk := handle.read(CHUNK_SIZE):
                yield chunk

    def read_bytes(self, uri: str) -> bytes:
        return Path(uri).read_bytes()

    def put_bytes(self, uri: str, data: bytes) -> None:
        target = Path(uri)
        target.parent.mkdir(parents=True, exist_ok=True)
        # Write-then-rename: a reader listing the directory never sees a half-written file.
        tmp = target.parent / f".{target.name}.tmp"
        tmp.write_bytes(data)
        tmp.replace(target)

    def exists(self, uri: str) -> bool:
        return Path(uri).exists()

    def delete(self, uri: str) -> None:
        path = Path(uri)
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()

    def ensure_container(self, uri: str) -> None:
        Path(uri).mkdir(parents=True, exist_ok=True)


class S3Store:
    def __init__(self, settings: StorageSettings, client: Any | None = None) -> None:
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                endpoint_url=settings.s3_endpoint_url,
                region_name=settings.s3_region,
                aws_access_key_id=settings.s3_access_key,
                aws_secret_access_key=settings.s3_secret_key,
                config=Config(
                    s3={"addressing_style": "path" if settings.s3_path_style else "auto"},
                    retries={"max_attempts": 5, "mode": "standard"},
                ),
            )
        self._client = client

    @staticmethod
    def split(uri: str) -> tuple[str, str]:
        without_scheme = uri.split("://", 1)[1]
        bucket, _, key = without_scheme.partition("/")
        return bucket, key

    def list_files(self, prefix_uri: str) -> list[StoredFile]:
        bucket, prefix = self.split(prefix_uri)
        prefix = prefix.rstrip("/") + "/" if prefix else ""
        files = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                relative = obj["Key"][len(prefix) :]
                if obj["Key"].endswith("/") or any(p.startswith((".", "_")) for p in relative.split("/")):
                    continue
                files.append(StoredFile(f"s3://{bucket}/{obj['Key']}", obj["Size"], obj["LastModified"]))
        return sorted(files, key=lambda f: f.uri)

    def iter_chunks(self, uri: str) -> Iterator[bytes]:
        bucket, key = self.split(uri)
        body = self._client.get_object(Bucket=bucket, Key=key)["Body"]
        yield from body.iter_chunks(CHUNK_SIZE)

    def read_bytes(self, uri: str) -> bytes:
        bucket, key = self.split(uri)
        data: bytes = self._client.get_object(Bucket=bucket, Key=key)["Body"].read()
        return data

    def put_bytes(self, uri: str, data: bytes) -> None:
        bucket, key = self.split(uri)
        # S3 PUT is atomic: the object becomes visible only once fully written.
        self._client.put_object(Bucket=bucket, Key=key, Body=data)

    def exists(self, uri: str) -> bool:
        from botocore.exceptions import ClientError

        bucket, key = self.split(uri)
        try:
            self._client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def delete(self, uri: str) -> None:
        bucket, prefix = self.split(uri)
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if keys:
                self._client.delete_objects(Bucket=bucket, Delete={"Objects": keys})

    def ensure_container(self, uri: str) -> None:
        from botocore.exceptions import ClientError

        bucket, _ = self.split(uri)
        try:
            self._client.head_bucket(Bucket=bucket)
        except ClientError:
            self._client.create_bucket(Bucket=bucket)


def get_store(uri: str, settings: StorageSettings) -> ObjectStore:
    return S3Store(settings) if is_s3(uri) else LocalStore()
