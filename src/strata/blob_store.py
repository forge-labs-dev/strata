"""Blob storage abstraction for artifact data.

This module provides a protocol for blob storage, enabling artifact data
to be stored on different backends (local filesystem, S3, GCS, etc.).

The BlobStore protocol defines three core operations:
- write_blob: Store artifact data
- read_blob: Retrieve artifact data
- blob_exists: Check if artifact data exists

Implementations:
- LocalBlobStore: Local filesystem storage (default)
- S3BlobStore: Amazon S3 / S3-compatible storage (MinIO, LocalStack)
- GCSBlobStore: Google Cloud Storage
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from strata.config import StrataConfig


class BlobStore(ABC):
    """Abstract base class for blob storage backends.

    Blob stores persist artifact data (Arrow IPC files) separately from
    metadata (SQLite). This separation enables:
    - Horizontal scaling of blob storage (S3/GCS)
    - Efficient data locality in distributed setups
    - Independent lifecycle management of data vs metadata

    All blob operations use (artifact_id, version) as the key, which maps
    to a unique blob path/key in the underlying storage.
    """

    @abstractmethod
    def write_blob(self, artifact_id: str, version: int, data: bytes) -> None:
        """Write artifact data to storage.

        Implementations should ensure atomic writes where possible (write to
        temp location then rename) to prevent partial writes on failure.

        Args:
            artifact_id: Unique artifact identifier
            version: Artifact version number
            data: Arrow IPC stream bytes to store
        """
        ...

    @abstractmethod
    def read_blob(self, artifact_id: str, version: int) -> bytes | None:
        """Read artifact data from storage.

        Args:
            artifact_id: Unique artifact identifier
            version: Artifact version number

        Returns:
            Arrow IPC stream bytes, or None if blob doesn't exist
        """
        ...

    @abstractmethod
    def blob_exists(self, artifact_id: str, version: int) -> bool:
        """Check if a blob exists in storage.

        Args:
            artifact_id: Unique artifact identifier
            version: Artifact version number

        Returns:
            True if the blob exists, False otherwise
        """
        ...

    @abstractmethod
    def delete_blob(self, artifact_id: str, version: int) -> bool:
        """Delete a blob from storage.

        Args:
            artifact_id: Unique artifact identifier
            version: Artifact version number

        Returns:
            True if blob was deleted, False if it didn't exist
        """
        ...

    def _blob_key(self, artifact_id: str, version: int) -> str:
        """Generate storage key for a blob.

        Default key format: {artifact_id}@v={version}.arrow

        Args:
            artifact_id: Unique artifact identifier
            version: Artifact version number

        Returns:
            Storage key string
        """
        return f"{artifact_id}@v={version}.arrow"


class LocalBlobStore(BlobStore):
    """Local filesystem blob storage.

    Stores blobs in a directory structure:
        {blobs_dir}/{artifact_id}@v={version}.arrow

    Uses atomic writes (write to .tmp, then rename) to prevent partial writes.
    """

    def __init__(self, blobs_dir: Path):
        """Initialize local blob store.

        Args:
            blobs_dir: Directory for storing blob files
        """
        self.blobs_dir = blobs_dir
        self.blobs_dir.mkdir(parents=True, exist_ok=True)

    def _blob_path(self, artifact_id: str, version: int) -> Path:
        """Get filesystem path for a blob."""
        return self.blobs_dir / self._blob_key(artifact_id, version)

    def write_blob(self, artifact_id: str, version: int, data: bytes) -> None:
        """Write blob to local filesystem with atomic rename."""
        path = self._blob_path(artifact_id, version)
        temp_path = path.with_suffix(".tmp")
        temp_path.write_bytes(data)
        temp_path.rename(path)

    def read_blob(self, artifact_id: str, version: int) -> bytes | None:
        """Read blob from local filesystem."""
        path = self._blob_path(artifact_id, version)
        if not path.exists():
            return None
        return path.read_bytes()

    def blob_exists(self, artifact_id: str, version: int) -> bool:
        """Check if blob exists on local filesystem."""
        return self._blob_path(artifact_id, version).exists()

    def delete_blob(self, artifact_id: str, version: int) -> bool:
        """Delete blob from local filesystem."""
        path = self._blob_path(artifact_id, version)
        if not path.exists():
            return False
        path.unlink()
        return True


class S3BlobStore(BlobStore):
    """Amazon S3 / S3-compatible blob storage.

    Stores blobs in an S3 bucket with key structure:
        {prefix}/{artifact_id}@v={version}.arrow

    Uses PyArrow's S3FileSystem for efficient Arrow IPC I/O.
    Supports S3-compatible services like MinIO, LocalStack, etc.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "artifacts",
        region: str | None = None,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        anonymous: bool = False,
    ):
        """Initialize S3 blob store.

        Args:
            bucket: S3 bucket name
            prefix: Key prefix within bucket (default: "artifacts")
            region: AWS region (e.g., "us-east-1")
            endpoint_url: Custom endpoint for S3-compatible services
            access_key: AWS access key ID (or use env/IAM)
            secret_key: AWS secret access key (or use env/IAM)
            anonymous: Use anonymous access for public buckets
        """
        import pyarrow.fs as pafs

        self.bucket = bucket
        self.prefix = prefix.strip("/")

        # Build S3FileSystem
        kwargs = {}
        if region:
            kwargs["region"] = region
        if endpoint_url:
            kwargs["endpoint_override"] = endpoint_url
        if access_key and secret_key:
            kwargs["access_key"] = access_key
            kwargs["secret_key"] = secret_key
        if anonymous:
            kwargs["anonymous"] = True

        self._fs = pafs.S3FileSystem(**kwargs)

    def _s3_key(self, artifact_id: str, version: int) -> str:
        """Get full S3 key for a blob."""
        blob_key = self._blob_key(artifact_id, version)
        if self.prefix:
            return f"{self.bucket}/{self.prefix}/{blob_key}"
        return f"{self.bucket}/{blob_key}"

    def write_blob(self, artifact_id: str, version: int, data: bytes) -> None:
        """Write blob to S3."""
        key = self._s3_key(artifact_id, version)
        with self._fs.open_output_stream(key) as f:
            f.write(data)

    def read_blob(self, artifact_id: str, version: int) -> bytes | None:
        """Read blob from S3."""
        import pyarrow as pa

        key = self._s3_key(artifact_id, version)
        try:
            with self._fs.open_input_stream(key) as f:
                return f.read()
        except (FileNotFoundError, pa.ArrowIOError):
            return None

    def blob_exists(self, artifact_id: str, version: int) -> bool:
        """Check if blob exists in S3."""
        import pyarrow.fs as pafs

        key = self._s3_key(artifact_id, version)
        try:
            info = self._fs.get_file_info(key)
            return info.type == pafs.FileType.File
        except Exception:
            return False

    def delete_blob(self, artifact_id: str, version: int) -> bool:
        """Delete blob from S3."""
        key = self._s3_key(artifact_id, version)
        try:
            if not self.blob_exists(artifact_id, version):
                return False
            self._fs.delete_file(key)
            return True
        except Exception:
            return False

    @classmethod
    def from_config(
        cls, config: StrataConfig, bucket: str, prefix: str = "artifacts"
    ) -> S3BlobStore:
        """Create S3BlobStore from Strata configuration.

        Args:
            config: Strata configuration with S3 settings
            bucket: S3 bucket name
            prefix: Key prefix within bucket

        Returns:
            Configured S3BlobStore instance
        """
        return cls(
            bucket=bucket,
            prefix=prefix,
            region=config.s3_region,
            endpoint_url=config.s3_endpoint_url,
            access_key=config.s3_access_key,
            secret_key=config.s3_secret_key,
            anonymous=config.s3_anonymous,
        )


class GCSBlobStore(BlobStore):
    """Google Cloud Storage blob storage.

    Stores blobs in a GCS bucket with key structure:
        {prefix}/{artifact_id}@v={version}.arrow

    Uses PyArrow's GcsFileSystem for efficient Arrow IPC I/O.
    Supports Application Default Credentials, service account keys, or anonymous access.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "artifacts",
        project_id: str | None = None,
        credentials_json: str | None = None,
        anonymous: bool = False,
        endpoint_override: str | None = None,
    ):
        """Initialize GCS blob store.

        Args:
            bucket: GCS bucket name
            prefix: Key prefix within bucket (default: "artifacts")
            project_id: GCP project ID (uses default if not specified)
            credentials_json: Path to service account JSON key file
            anonymous: Use anonymous access for public buckets
            endpoint_override: Custom endpoint for GCS-compatible services (e.g., fake-gcs-server)
        """
        import pyarrow.fs as pafs

        self.bucket = bucket
        self.prefix = prefix.strip("/")

        # Build GcsFileSystem
        kwargs = {}
        if project_id:
            kwargs["default_bucket_location"] = project_id
        if credentials_json:
            kwargs["access_token"] = None  # Disable token auth
            # GcsFileSystem uses GOOGLE_APPLICATION_CREDENTIALS env var
            # or explicit credentials via access_token
            # For service account key files, set the env var before creating
            import os

            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_json
        if anonymous:
            kwargs["anonymous"] = True
        if endpoint_override:
            kwargs["endpoint_override"] = endpoint_override

        self._fs = pafs.GcsFileSystem(**kwargs)

    def _gcs_key(self, artifact_id: str, version: int) -> str:
        """Get full GCS key for a blob."""
        blob_key = self._blob_key(artifact_id, version)
        if self.prefix:
            return f"{self.bucket}/{self.prefix}/{blob_key}"
        return f"{self.bucket}/{blob_key}"

    def write_blob(self, artifact_id: str, version: int, data: bytes) -> None:
        """Write blob to GCS."""
        key = self._gcs_key(artifact_id, version)
        with self._fs.open_output_stream(key) as f:
            f.write(data)

    def read_blob(self, artifact_id: str, version: int) -> bytes | None:
        """Read blob from GCS."""
        import pyarrow as pa

        key = self._gcs_key(artifact_id, version)
        try:
            with self._fs.open_input_stream(key) as f:
                return f.read()
        except (FileNotFoundError, pa.ArrowIOError):
            return None

    def blob_exists(self, artifact_id: str, version: int) -> bool:
        """Check if blob exists in GCS."""
        import pyarrow.fs as pafs

        key = self._gcs_key(artifact_id, version)
        try:
            info = self._fs.get_file_info(key)
            return info.type == pafs.FileType.File
        except Exception:
            return False

    def delete_blob(self, artifact_id: str, version: int) -> bool:
        """Delete blob from GCS."""
        key = self._gcs_key(artifact_id, version)
        try:
            if not self.blob_exists(artifact_id, version):
                return False
            self._fs.delete_file(key)
            return True
        except Exception:
            return False

    @classmethod
    def from_config(
        cls, config: StrataConfig, bucket: str, prefix: str = "artifacts"
    ) -> GCSBlobStore:
        """Create GCSBlobStore from Strata configuration.

        Args:
            config: Strata configuration with GCS settings
            bucket: GCS bucket name
            prefix: Key prefix within bucket

        Returns:
            Configured GCSBlobStore instance
        """
        return cls(
            bucket=bucket,
            prefix=prefix,
            project_id=config.gcs_project_id,
            credentials_json=config.gcs_credentials_json,
            anonymous=config.gcs_anonymous,
            endpoint_override=config.gcs_endpoint_override,
        )


def create_blob_store(config: StrataConfig) -> BlobStore:
    """Create appropriate blob store based on configuration.

    Factory function that creates the correct blob store implementation
    based on configuration settings.

    Configuration options (in pyproject.toml or environment):
        # Local filesystem (default)
        artifact_dir = "/path/to/artifacts"

        # S3 storage
        artifact_blob_backend = "s3"
        artifact_s3_bucket = "my-bucket"
        artifact_s3_prefix = "artifacts"

        # GCS storage
        artifact_blob_backend = "gcs"
        artifact_gcs_bucket = "my-bucket"
        artifact_gcs_prefix = "artifacts"

    Environment variables:
        STRATA_ARTIFACT_BLOB_BACKEND: "local" | "s3" | "gcs"
        STRATA_ARTIFACT_S3_BUCKET: S3 bucket name
        STRATA_ARTIFACT_S3_PREFIX: Key prefix in bucket
        STRATA_ARTIFACT_GCS_BUCKET: GCS bucket name
        STRATA_ARTIFACT_GCS_PREFIX: Key prefix in bucket

    Args:
        config: Strata configuration

    Returns:
        Configured BlobStore instance

    Raises:
        ValueError: If required configuration is missing
    """
    # Check environment variables first
    backend = os.environ.get("STRATA_ARTIFACT_BLOB_BACKEND", "local").lower()

    if backend == "s3":
        bucket = os.environ.get("STRATA_ARTIFACT_S3_BUCKET")
        if not bucket:
            raise ValueError(
                "S3 blob backend requires STRATA_ARTIFACT_S3_BUCKET environment variable"
            )
        prefix = os.environ.get("STRATA_ARTIFACT_S3_PREFIX", "artifacts")
        return S3BlobStore.from_config(config, bucket=bucket, prefix=prefix)

    if backend == "gcs":
        bucket = os.environ.get("STRATA_ARTIFACT_GCS_BUCKET")
        if not bucket:
            raise ValueError(
                "GCS blob backend requires STRATA_ARTIFACT_GCS_BUCKET environment variable"
            )
        prefix = os.environ.get("STRATA_ARTIFACT_GCS_PREFIX", "artifacts")
        return GCSBlobStore.from_config(config, bucket=bucket, prefix=prefix)

    # Default: local filesystem
    if config.artifact_dir is None:
        raise ValueError("Local blob store requires artifact_dir in configuration")
    blobs_dir = config.artifact_dir / "blobs"
    return LocalBlobStore(blobs_dir)
