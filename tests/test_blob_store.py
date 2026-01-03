"""Tests for blob storage backends."""

from pathlib import Path

import pytest

from strata.blob_store import (
    GCSBlobStore,
    LocalBlobStore,
    S3BlobStore,
    create_blob_store,
)
from strata.config import StrataConfig


class TestLocalBlobStore:
    """Tests for LocalBlobStore."""

    def test_write_and_read_blob(self, tmp_path: Path):
        """Test writing and reading a blob."""
        store = LocalBlobStore(tmp_path / "blobs")
        data = b"test artifact data"

        store.write_blob("artifact-1", 1, data)
        result = store.read_blob("artifact-1", 1)

        assert result == data

    def test_read_nonexistent_blob(self, tmp_path: Path):
        """Test reading a blob that doesn't exist."""
        store = LocalBlobStore(tmp_path / "blobs")

        result = store.read_blob("nonexistent", 1)

        assert result is None

    def test_blob_exists(self, tmp_path: Path):
        """Test checking if a blob exists."""
        store = LocalBlobStore(tmp_path / "blobs")
        data = b"test data"

        assert not store.blob_exists("artifact-1", 1)

        store.write_blob("artifact-1", 1, data)

        assert store.blob_exists("artifact-1", 1)
        assert not store.blob_exists("artifact-1", 2)

    def test_delete_blob(self, tmp_path: Path):
        """Test deleting a blob."""
        store = LocalBlobStore(tmp_path / "blobs")
        data = b"test data"

        store.write_blob("artifact-1", 1, data)
        assert store.blob_exists("artifact-1", 1)

        result = store.delete_blob("artifact-1", 1)

        assert result is True
        assert not store.blob_exists("artifact-1", 1)

    def test_delete_nonexistent_blob(self, tmp_path: Path):
        """Test deleting a blob that doesn't exist."""
        store = LocalBlobStore(tmp_path / "blobs")

        result = store.delete_blob("nonexistent", 1)

        assert result is False

    def test_multiple_versions(self, tmp_path: Path):
        """Test storing multiple versions of the same artifact."""
        store = LocalBlobStore(tmp_path / "blobs")

        store.write_blob("artifact-1", 1, b"version 1")
        store.write_blob("artifact-1", 2, b"version 2")
        store.write_blob("artifact-1", 3, b"version 3")

        assert store.read_blob("artifact-1", 1) == b"version 1"
        assert store.read_blob("artifact-1", 2) == b"version 2"
        assert store.read_blob("artifact-1", 3) == b"version 3"

    def test_blob_key_format(self, tmp_path: Path):
        """Test the blob key format."""
        store = LocalBlobStore(tmp_path / "blobs")

        key = store._blob_key("abc123", 5)

        assert key == "abc123@v=5.arrow"

    def test_creates_directory(self, tmp_path: Path):
        """Test that the store creates the blobs directory."""
        blobs_dir = tmp_path / "new" / "nested" / "blobs"
        assert not blobs_dir.exists()

        LocalBlobStore(blobs_dir)

        assert blobs_dir.exists()

    def test_atomic_write(self, tmp_path: Path):
        """Test that writes are atomic (no partial files on failure)."""
        store = LocalBlobStore(tmp_path / "blobs")
        data = b"test data" * 1000

        store.write_blob("artifact-1", 1, data)

        # The temp file should not exist
        temp_path = store._blob_path("artifact-1", 1).with_suffix(".tmp")
        assert not temp_path.exists()

        # The final file should exist with correct content
        assert store.read_blob("artifact-1", 1) == data


class TestS3BlobStore:
    """Tests for S3BlobStore.

    Note: PyArrow's S3FileSystem uses its own C++ AWS SDK which doesn't
    work with moto mock. These tests verify the key generation logic and
    read behavior. Full S3 integration requires actual S3 or LocalStack.
    """

    @pytest.fixture
    def s3_store_mock(self):
        """Create an S3BlobStore with mocked S3 for key tests."""
        pytest.importorskip("moto")
        import boto3
        from moto import mock_aws

        with mock_aws():
            conn = boto3.client("s3", region_name="us-east-1")
            conn.create_bucket(Bucket="test-bucket")

            store = S3BlobStore(
                bucket="test-bucket",
                prefix="artifacts",
                region="us-east-1",
            )
            yield store

    def test_s3_key_format(self, s3_store_mock: S3BlobStore):
        """Test the S3 key format includes bucket and prefix."""
        key = s3_store_mock._s3_key("abc123", 5)

        assert key == "test-bucket/artifacts/abc123@v=5.arrow"

    def test_s3_key_without_prefix(self):
        """Test S3 key format without prefix."""
        pytest.importorskip("moto")
        import boto3
        from moto import mock_aws

        with mock_aws():
            conn = boto3.client("s3", region_name="us-east-1")
            conn.create_bucket(Bucket="test-bucket")

            store = S3BlobStore(
                bucket="test-bucket",
                prefix="",
                region="us-east-1",
            )

            key = store._s3_key("abc123", 5)
            assert key == "test-bucket/abc123@v=5.arrow"

    def test_blob_key_format(self, s3_store_mock: S3BlobStore):
        """Test the blob key format."""
        key = s3_store_mock._blob_key("abc123", 5)

        assert key == "abc123@v=5.arrow"

    def test_read_nonexistent_blob(self, s3_store_mock: S3BlobStore):
        """Test reading a blob that doesn't exist in S3.

        Note: PyArrow's S3FileSystem doesn't work with moto for writes,
        but read of nonexistent files should return None.
        """
        result = s3_store_mock.read_blob("nonexistent", 1)

        assert result is None

    def test_blob_exists_nonexistent(self, s3_store_mock: S3BlobStore):
        """Test checking if a nonexistent blob exists in S3."""
        assert not s3_store_mock.blob_exists("nonexistent", 1)

    @pytest.mark.skip(reason="Requires actual S3/LocalStack - PyArrow doesn't work with moto")
    def test_write_and_read_blob_integration(self):
        """Test writing and reading a blob from actual S3.

        This test is skipped by default. To run it, start LocalStack and set:
            STRATA_S3_ENDPOINT_URL=http://localhost:4566
            STRATA_S3_REGION=us-east-1
        """
        import os

        endpoint = os.environ.get("STRATA_S3_ENDPOINT_URL")
        if not endpoint:
            pytest.skip("STRATA_S3_ENDPOINT_URL not set")

        store = S3BlobStore(
            bucket="test-bucket",
            prefix="artifacts",
            region="us-east-1",
            endpoint_url=endpoint,
            access_key="test",
            secret_key="test",
        )

        data = b"test artifact data"
        store.write_blob("artifact-1", 1, data)
        result = store.read_blob("artifact-1", 1)

        assert result == data


class TestGCSBlobStore:
    """Tests for GCSBlobStore.

    Note: PyArrow's GcsFileSystem uses its own C++ GCS SDK which doesn't
    work with mock libraries. These tests verify the key generation logic.
    Full GCS integration requires actual GCS or fake-gcs-server.
    """

    def test_gcs_key_format(self):
        """Test the GCS key format includes bucket and prefix."""
        # Create store with anonymous access to avoid credential errors
        store = GCSBlobStore(
            bucket="test-bucket",
            prefix="artifacts",
            anonymous=True,
        )

        key = store._gcs_key("abc123", 5)

        assert key == "test-bucket/artifacts/abc123@v=5.arrow"

    def test_gcs_key_without_prefix(self):
        """Test GCS key format without prefix."""
        store = GCSBlobStore(
            bucket="test-bucket",
            prefix="",
            anonymous=True,
        )

        key = store._gcs_key("abc123", 5)
        assert key == "test-bucket/abc123@v=5.arrow"

    def test_blob_key_format(self):
        """Test the blob key format."""
        store = GCSBlobStore(
            bucket="test-bucket",
            prefix="artifacts",
            anonymous=True,
        )

        key = store._blob_key("abc123", 5)

        assert key == "abc123@v=5.arrow"

    def test_read_nonexistent_blob(self):
        """Test reading a blob that doesn't exist in GCS.

        Note: This will fail to connect to GCS but should return None
        due to exception handling.
        """
        store = GCSBlobStore(
            bucket="nonexistent-bucket-xyz123",
            prefix="artifacts",
            anonymous=True,
        )

        # This should return None due to connection/not-found errors
        result = store.read_blob("nonexistent", 1)

        assert result is None

    def test_blob_exists_nonexistent(self):
        """Test checking if a nonexistent blob exists in GCS."""
        store = GCSBlobStore(
            bucket="nonexistent-bucket-xyz123",
            prefix="artifacts",
            anonymous=True,
        )

        # This should return False due to connection/not-found errors
        assert not store.blob_exists("nonexistent", 1)

    @pytest.mark.skip(reason="Requires actual GCS or fake-gcs-server")
    def test_write_and_read_blob_integration(self):
        """Test writing and reading a blob from actual GCS.

        This test is skipped by default. To run it, start fake-gcs-server and set:
            STRATA_GCS_ENDPOINT_OVERRIDE=http://localhost:4443
            STRATA_GCS_ANONYMOUS=true

        Or use actual GCS with GOOGLE_APPLICATION_CREDENTIALS.
        """
        import os

        endpoint = os.environ.get("STRATA_GCS_ENDPOINT_OVERRIDE")
        if not endpoint:
            pytest.skip("STRATA_GCS_ENDPOINT_OVERRIDE not set")

        store = GCSBlobStore(
            bucket="test-bucket",
            prefix="artifacts",
            endpoint_override=endpoint,
            anonymous=True,
        )

        data = b"test artifact data"
        store.write_blob("artifact-1", 1, data)
        result = store.read_blob("artifact-1", 1)

        assert result == data


class TestAzureBlobStore:
    """Tests for AzureBlobStore.

    Note: The Azure SDK doesn't have great mocking support like moto.
    These tests verify the key generation logic and error handling.
    Full Azure integration requires actual Azure Storage or Azurite emulator.
    """

    def test_azure_key_format_with_prefix(self):
        """Test the Azure key format includes prefix."""
        pytest.importorskip("azure.storage.blob")
        from strata.blob_store import AzureBlobStore

        # Create a store using connection string (won't actually connect)
        # We use a fake connection string format for testing key generation
        store = AzureBlobStore(
            account_name="testaccount",
            container_name="test-container",
            prefix="artifacts",
            connection_string="DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net",
        )

        key = store._azure_key("abc123", 5)

        assert key == "artifacts/abc123@v=5.arrow"

    def test_azure_key_format_without_prefix(self):
        """Test Azure key format without prefix."""
        pytest.importorskip("azure.storage.blob")
        from strata.blob_store import AzureBlobStore

        store = AzureBlobStore(
            account_name="testaccount",
            container_name="test-container",
            prefix="",
            connection_string="DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net",
        )

        key = store._azure_key("abc123", 5)

        assert key == "abc123@v=5.arrow"

    def test_blob_key_format(self):
        """Test the blob key format."""
        pytest.importorskip("azure.storage.blob")
        from strata.blob_store import AzureBlobStore

        store = AzureBlobStore(
            account_name="testaccount",
            container_name="test-container",
            prefix="artifacts",
            connection_string="DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net",
        )

        key = store._blob_key("abc123", 5)

        assert key == "abc123@v=5.arrow"

    def test_requires_auth_method(self):
        """Test that Azure store raises error without auth method."""
        pytest.importorskip("azure.storage.blob")
        from strata.blob_store import AzureBlobStore

        with pytest.raises(ValueError, match="requires one of"):
            AzureBlobStore(
                account_name="testaccount",
                container_name="test-container",
                prefix="artifacts",
                # No auth method provided
            )

    def test_import_error_message_contents(self):
        """Test that AzureBlobStore has a helpful error message for missing package."""
        # This test verifies the error message text exists in the source
        # without actually triggering module manipulation that causes test pollution
        import inspect

        pytest.importorskip("azure.storage.blob")
        from strata.blob_store import AzureBlobStore

        source = inspect.getsource(AzureBlobStore.__init__)
        assert "azure" in source.lower()
        assert "pip install strata[azure]" in source

    @pytest.mark.skip(reason="Requires actual Azure Storage or Azurite emulator")
    def test_write_and_read_blob_integration(self):
        """Test writing and reading a blob from actual Azure Storage.

        This test is skipped by default. To run it, start Azurite and set:
            STRATA_AZURE_CONNECTION_STRING=UseDevelopmentStorage=true

        Or use actual Azure Storage with a connection string.
        """
        import os

        pytest.importorskip("azure.storage.blob")
        from strata.blob_store import AzureBlobStore

        connection_string = os.environ.get("STRATA_AZURE_CONNECTION_STRING")
        if not connection_string:
            pytest.skip("STRATA_AZURE_CONNECTION_STRING not set")

        store = AzureBlobStore(
            account_name="devstoreaccount1",  # Azurite default
            container_name="test-container",
            prefix="artifacts",
            connection_string=connection_string,
        )

        data = b"test artifact data"
        store.write_blob("artifact-1", 1, data)
        result = store.read_blob("artifact-1", 1)

        assert result == data


class TestCreateBlobStore:
    """Tests for the create_blob_store factory function."""

    def test_creates_local_store_by_default(self, tmp_path: Path, monkeypatch):
        """Test that local store is created by default."""
        # Clear any env vars
        monkeypatch.delenv("STRATA_ARTIFACT_BLOB_BACKEND", raising=False)
        monkeypatch.delenv("STRATA_ARTIFACT_S3_BUCKET", raising=False)

        config = StrataConfig(
            deployment_mode="personal",
            artifact_dir=tmp_path / "artifacts",
        )

        store = create_blob_store(config)

        assert isinstance(store, LocalBlobStore)

    def test_creates_s3_store_from_env(self, tmp_path: Path, monkeypatch):
        """Test that S3 store is created when configured via env."""
        pytest.importorskip("moto")
        import boto3
        from moto import mock_aws

        with mock_aws():
            conn = boto3.client("s3", region_name="us-east-1")
            conn.create_bucket(Bucket="my-bucket")

            monkeypatch.setenv("STRATA_ARTIFACT_BLOB_BACKEND", "s3")
            monkeypatch.setenv("STRATA_ARTIFACT_S3_BUCKET", "my-bucket")
            monkeypatch.setenv("STRATA_ARTIFACT_S3_PREFIX", "custom-prefix")

            config = StrataConfig(
                deployment_mode="personal",
                artifact_dir=tmp_path / "artifacts",
                s3_region="us-east-1",
            )

            store = create_blob_store(config)

            assert isinstance(store, S3BlobStore)
            assert store.bucket == "my-bucket"
            assert store.prefix == "custom-prefix"

    def test_raises_without_s3_bucket(self, tmp_path: Path, monkeypatch):
        """Test that S3 store raises error without bucket."""
        monkeypatch.setenv("STRATA_ARTIFACT_BLOB_BACKEND", "s3")
        monkeypatch.delenv("STRATA_ARTIFACT_S3_BUCKET", raising=False)

        config = StrataConfig(
            deployment_mode="personal",
            artifact_dir=tmp_path / "artifacts",
        )

        with pytest.raises(ValueError, match="S3 blob backend requires"):
            create_blob_store(config)

    def test_raises_without_artifact_dir(self, monkeypatch):
        """Test that local store raises error without artifact_dir."""
        monkeypatch.delenv("STRATA_ARTIFACT_BLOB_BACKEND", raising=False)

        config = StrataConfig(
            deployment_mode="service",
            artifact_dir=None,
        )

        with pytest.raises(ValueError, match="requires artifact_dir"):
            create_blob_store(config)

    def test_creates_gcs_store_from_env(self, tmp_path: Path, monkeypatch):
        """Test that GCS store is created when configured via env."""
        monkeypatch.setenv("STRATA_ARTIFACT_BLOB_BACKEND", "gcs")
        monkeypatch.setenv("STRATA_ARTIFACT_GCS_BUCKET", "my-gcs-bucket")
        monkeypatch.setenv("STRATA_ARTIFACT_GCS_PREFIX", "custom-prefix")
        monkeypatch.setenv("STRATA_GCS_ANONYMOUS", "true")

        config = StrataConfig(
            deployment_mode="personal",
            artifact_dir=tmp_path / "artifacts",
            gcs_anonymous=True,
        )

        store = create_blob_store(config)

        assert isinstance(store, GCSBlobStore)
        assert store.bucket == "my-gcs-bucket"
        assert store.prefix == "custom-prefix"

    def test_raises_without_gcs_bucket(self, tmp_path: Path, monkeypatch):
        """Test that GCS store raises error without bucket."""
        monkeypatch.setenv("STRATA_ARTIFACT_BLOB_BACKEND", "gcs")
        monkeypatch.delenv("STRATA_ARTIFACT_GCS_BUCKET", raising=False)

        config = StrataConfig(
            deployment_mode="personal",
            artifact_dir=tmp_path / "artifacts",
        )

        with pytest.raises(ValueError, match="GCS blob backend requires"):
            create_blob_store(config)

    def test_creates_azure_store_from_env(self, tmp_path: Path, monkeypatch):
        """Test that Azure store is created when configured via env."""
        pytest.importorskip("azure.storage.blob")
        from strata.blob_store import AzureBlobStore

        monkeypatch.setenv("STRATA_ARTIFACT_BLOB_BACKEND", "azure")
        monkeypatch.setenv("STRATA_ARTIFACT_AZURE_CONTAINER", "my-container")
        monkeypatch.setenv("STRATA_ARTIFACT_AZURE_PREFIX", "custom-prefix")

        config = StrataConfig(
            deployment_mode="personal",
            artifact_dir=tmp_path / "artifacts",
            azure_connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
        )

        store = create_blob_store(config)

        assert isinstance(store, AzureBlobStore)
        assert store.container_name == "my-container"
        assert store.prefix == "custom-prefix"

    def test_raises_without_azure_container(self, tmp_path: Path, monkeypatch):
        """Test that Azure store raises error without container."""
        monkeypatch.setenv("STRATA_ARTIFACT_BLOB_BACKEND", "azure")
        monkeypatch.delenv("STRATA_ARTIFACT_AZURE_CONTAINER", raising=False)

        config = StrataConfig(
            deployment_mode="personal",
            artifact_dir=tmp_path / "artifacts",
        )

        with pytest.raises(ValueError, match="Azure blob backend requires"):
            create_blob_store(config)


class TestConfigCreateBlobStore:
    """Tests for StrataConfig.create_blob_store() method."""

    def test_creates_local_store(self, tmp_path: Path):
        """Test creating local store from config."""
        config = StrataConfig(
            deployment_mode="personal",
            artifact_dir=tmp_path / "artifacts",
            artifact_blob_backend="local",
        )

        store = config.create_blob_store()

        assert isinstance(store, LocalBlobStore)

    def test_creates_s3_store(self, tmp_path: Path):
        """Test creating S3 store from config."""
        pytest.importorskip("moto")
        import boto3
        from moto import mock_aws

        with mock_aws():
            conn = boto3.client("s3", region_name="us-east-1")
            conn.create_bucket(Bucket="my-bucket")

            config = StrataConfig(
                deployment_mode="personal",
                artifact_dir=tmp_path / "artifacts",
                artifact_blob_backend="s3",
                artifact_s3_bucket="my-bucket",
                artifact_s3_prefix="my-prefix",
                s3_region="us-east-1",
            )

            store = config.create_blob_store()

            assert isinstance(store, S3BlobStore)
            assert store.bucket == "my-bucket"
            assert store.prefix == "my-prefix"

    def test_raises_without_s3_bucket(self, tmp_path: Path):
        """Test that S3 store raises error without bucket in config."""
        config = StrataConfig(
            deployment_mode="personal",
            artifact_dir=tmp_path / "artifacts",
            artifact_blob_backend="s3",
            artifact_s3_bucket=None,
        )

        with pytest.raises(ValueError, match="requires artifact_s3_bucket"):
            config.create_blob_store()

    def test_creates_gcs_store(self, tmp_path: Path):
        """Test creating GCS store from config."""
        config = StrataConfig(
            deployment_mode="personal",
            artifact_dir=tmp_path / "artifacts",
            artifact_blob_backend="gcs",
            artifact_gcs_bucket="my-gcs-bucket",
            artifact_gcs_prefix="my-prefix",
            gcs_anonymous=True,
        )

        store = config.create_blob_store()

        assert isinstance(store, GCSBlobStore)
        assert store.bucket == "my-gcs-bucket"
        assert store.prefix == "my-prefix"

    def test_raises_without_gcs_bucket(self, tmp_path: Path):
        """Test that GCS store raises error without bucket in config."""
        config = StrataConfig(
            deployment_mode="personal",
            artifact_dir=tmp_path / "artifacts",
            artifact_blob_backend="gcs",
            artifact_gcs_bucket=None,
        )

        with pytest.raises(ValueError, match="requires artifact_gcs_bucket"):
            config.create_blob_store()

    def test_creates_azure_store(self, tmp_path: Path):
        """Test creating Azure store from config."""
        pytest.importorskip("azure.storage.blob")
        from strata.blob_store import AzureBlobStore

        config = StrataConfig(
            deployment_mode="personal",
            artifact_dir=tmp_path / "artifacts",
            artifact_blob_backend="azure",
            artifact_azure_container="my-container",
            artifact_azure_prefix="my-prefix",
            azure_connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
        )

        store = config.create_blob_store()

        assert isinstance(store, AzureBlobStore)
        assert store.container_name == "my-container"
        assert store.prefix == "my-prefix"

    def test_raises_without_azure_container(self, tmp_path: Path):
        """Test that Azure store raises error without container in config."""
        config = StrataConfig(
            deployment_mode="personal",
            artifact_dir=tmp_path / "artifacts",
            artifact_blob_backend="azure",
            artifact_azure_container=None,
        )

        with pytest.raises(ValueError, match="requires artifact_azure_container"):
            config.create_blob_store()
