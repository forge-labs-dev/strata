"""Signed URL generation for pull-model executor execution.

Stage 2 of the executor protocol uses signed URLs so executors can:
1. Pull inputs directly from Strata's storage
2. Push outputs directly to Strata's storage

This decouples the data plane from Strata, enabling:
- Easier executor scaling (no bandwidth bottleneck at Strata)
- Native retries (executors retry failed downloads/uploads)
- Reduced memory pressure on Strata

Security:
- URLs are signed with HMAC-SHA256 using a server-side secret
- URLs include: build_id, operation, expiry, size_limit
- Signature prevents URL tampering
- Expiry prevents replay attacks
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlencode

if TYPE_CHECKING:
    pass


# Default signing secret - should be overridden in production
_signing_secret: bytes | None = None


def get_signing_secret() -> bytes:
    """Get the signing secret, generating one if needed."""
    global _signing_secret
    if _signing_secret is None:
        _signing_secret = secrets.token_bytes(32)
    return _signing_secret


def set_signing_secret(secret: bytes) -> None:
    """Set the signing secret (call at startup)."""
    global _signing_secret
    _signing_secret = secret


def reset_signing_secret() -> None:
    """Reset the signing secret (for testing)."""
    global _signing_secret
    _signing_secret = None


@dataclass(frozen=True)
class SignedDownloadURL:
    """Signed URL for downloading an input.

    Attributes:
        url: Full URL with signature query params
        artifact_id: Artifact ID being downloaded
        version: Version being downloaded
        expires_at: Unix timestamp when URL expires
    """

    url: str
    artifact_id: str
    version: int
    expires_at: float


@dataclass(frozen=True)
class SignedUploadURL:
    """Signed URL for uploading build output.

    Attributes:
        url: Full URL with signature query params
        build_id: Build ID the upload is for
        max_bytes: Maximum upload size in bytes
        expires_at: Unix timestamp when URL expires
    """

    url: str
    build_id: str
    max_bytes: int
    expires_at: float


@dataclass(frozen=True)
class SignedFinalizeURL:
    """Signed URL for finalizing a build output.

    Attributes:
        url: Full URL with signature query params
        build_id: Build ID the finalize is for
        expires_at: Unix timestamp when URL expires
    """

    url: str
    build_id: str
    expires_at: float


@dataclass(frozen=True)
class BuildManifest:
    """Manifest of signed URLs for a build.

    Sent to the executor so it can pull inputs and push output.

    Attributes:
        build_id: Build ID
        metadata: Build metadata (transform spec, params, etc.)
        input_urls: List of signed download URLs for inputs
        output_url: Signed upload URL for output
        finalize_url: URL to call after upload is complete
    """

    build_id: str
    metadata: dict
    input_urls: list[SignedDownloadURL]
    output_url: SignedUploadURL
    finalize_url: str

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "build_id": self.build_id,
            "metadata": self.metadata,
            "inputs": [
                {
                    "url": url.url,
                    "artifact_id": url.artifact_id,
                    "version": url.version,
                    "expires_at": url.expires_at,
                }
                for url in self.input_urls
            ],
            "output": {
                "url": self.output_url.url,
                "max_bytes": self.output_url.max_bytes,
                "expires_at": self.output_url.expires_at,
            },
            "finalize_url": self.finalize_url,
        }


def _sign(data: dict, secret: bytes) -> str:
    """Sign data with HMAC-SHA256.

    Args:
        data: Dictionary to sign (will be JSON encoded)
        secret: Signing secret

    Returns:
        Base64-encoded signature
    """
    message = json.dumps(data, sort_keys=True).encode()
    signature = hmac.new(secret, message, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(signature).decode()


def _verify(data: dict, signature: str, secret: bytes) -> bool:
    """Verify HMAC-SHA256 signature.

    Args:
        data: Dictionary that was signed
        signature: Base64-encoded signature to verify
        secret: Signing secret

    Returns:
        True if signature is valid
    """
    expected = _sign(data, secret)
    return hmac.compare_digest(expected, signature)


def generate_download_url(
    base_url: str,
    artifact_id: str,
    version: int,
    build_id: str,
    expiry_seconds: float = 300.0,
) -> SignedDownloadURL:
    """Generate a signed URL for downloading an artifact.

    Args:
        base_url: Base URL of the Strata server (e.g., "http://localhost:8765")
        artifact_id: Artifact ID to download
        version: Version to download
        build_id: Build ID this download is for (for audit)
        expiry_seconds: URL validity in seconds (default 5 minutes)

    Returns:
        SignedDownloadURL with full URL and metadata
    """
    expires_at = time.time() + expiry_seconds

    # Data to sign
    data = {
        "op": "download",
        "artifact_id": artifact_id,
        "version": version,
        "build_id": build_id,
        "expires_at": expires_at,
    }

    signature = _sign(data, get_signing_secret())

    # Build URL with query params
    params = {
        "artifact_id": artifact_id,
        "version": str(version),
        "build_id": build_id,
        "expires_at": str(expires_at),
        "signature": signature,
    }

    url = f"{base_url}/v1/artifacts/download?{urlencode(params)}"

    return SignedDownloadURL(
        url=url,
        artifact_id=artifact_id,
        version=version,
        expires_at=expires_at,
    )


def generate_upload_url(
    base_url: str,
    build_id: str,
    max_bytes: int,
    expiry_seconds: float = 600.0,
) -> SignedUploadURL:
    """Generate a signed URL for uploading build output.

    Args:
        base_url: Base URL of the Strata server
        build_id: Build ID the upload is for
        max_bytes: Maximum upload size in bytes
        expiry_seconds: URL validity in seconds (default 10 minutes)

    Returns:
        SignedUploadURL with full URL and metadata
    """
    expires_at = time.time() + expiry_seconds

    # Data to sign
    data = {
        "op": "upload",
        "build_id": build_id,
        "max_bytes": max_bytes,
        "expires_at": expires_at,
    }

    signature = _sign(data, get_signing_secret())

    # Build URL with query params
    params = {
        "build_id": build_id,
        "max_bytes": str(max_bytes),
        "expires_at": str(expires_at),
        "signature": signature,
    }

    url = f"{base_url}/v1/artifacts/upload?{urlencode(params)}"

    return SignedUploadURL(
        url=url,
        build_id=build_id,
        max_bytes=max_bytes,
        expires_at=expires_at,
    )


def verify_download_signature(
    artifact_id: str,
    version: int,
    build_id: str,
    expires_at: float,
    signature: str,
) -> bool:
    """Verify a download URL signature.

    Args:
        artifact_id: Artifact ID from URL
        version: Version from URL
        build_id: Build ID from URL
        expires_at: Expiry timestamp from URL
        signature: Signature from URL

    Returns:
        True if signature is valid and not expired
    """
    # Check expiry
    if time.time() > expires_at:
        return False

    # Reconstruct signed data
    data = {
        "op": "download",
        "artifact_id": artifact_id,
        "version": version,
        "build_id": build_id,
        "expires_at": expires_at,
    }

    return _verify(data, signature, get_signing_secret())


def verify_upload_signature(
    build_id: str,
    max_bytes: int,
    expires_at: float,
    signature: str,
) -> bool:
    """Verify an upload URL signature.

    Args:
        build_id: Build ID from URL
        max_bytes: Max bytes from URL
        expires_at: Expiry timestamp from URL
        signature: Signature from URL

    Returns:
        True if signature is valid and not expired
    """
    # Check expiry
    if time.time() > expires_at:
        return False

    # Reconstruct signed data
    data = {
        "op": "upload",
        "build_id": build_id,
        "max_bytes": max_bytes,
        "expires_at": expires_at,
    }

    return _verify(data, signature, get_signing_secret())


def generate_finalize_url(
    base_url: str,
    build_id: str,
    expiry_seconds: float = 600.0,
) -> SignedFinalizeURL:
    """Generate a signed URL for finalizing a build."""
    expires_at = time.time() + expiry_seconds

    data = {
        "op": "finalize",
        "build_id": build_id,
        "expires_at": expires_at,
    }

    signature = _sign(data, get_signing_secret())
    params = {
        "expires_at": str(expires_at),
        "signature": signature,
    }
    url = f"{base_url}/v1/builds/{build_id}/finalize?{urlencode(params)}"

    return SignedFinalizeURL(
        url=url,
        build_id=build_id,
        expires_at=expires_at,
    )


def verify_finalize_signature(
    build_id: str,
    expires_at: float,
    signature: str,
) -> bool:
    """Verify a finalize URL signature."""
    if time.time() > expires_at:
        return False

    data = {
        "op": "finalize",
        "build_id": build_id,
        "expires_at": expires_at,
    }

    return _verify(data, signature, get_signing_secret())


def generate_build_manifest(
    base_url: str,
    build_id: str,
    metadata: dict,
    input_artifacts: list[tuple[str, int]],  # List of (artifact_id, version)
    max_output_bytes: int,
    url_expiry_seconds: float = 600.0,
) -> BuildManifest:
    """Generate a complete manifest for a build.

    This includes all the signed URLs the executor needs to:
    1. Download each input artifact
    2. Upload the output
    3. Finalize the build

    Args:
        base_url: Base URL of the Strata server
        build_id: Build ID
        metadata: Build metadata (transform spec, params, etc.)
        input_artifacts: List of (artifact_id, version) tuples for inputs
        max_output_bytes: Maximum output size in bytes
        url_expiry_seconds: How long URLs are valid

    Returns:
        BuildManifest with all signed URLs
    """
    # Generate download URLs for each input
    input_urls = [
        generate_download_url(
            base_url=base_url,
            artifact_id=artifact_id,
            version=version,
            build_id=build_id,
            expiry_seconds=url_expiry_seconds,
        )
        for artifact_id, version in input_artifacts
    ]

    # Generate upload URL for output
    output_url = generate_upload_url(
        base_url=base_url,
        build_id=build_id,
        max_bytes=max_output_bytes,
        expiry_seconds=url_expiry_seconds,
    )

    finalize_url = generate_finalize_url(
        base_url=base_url,
        build_id=build_id,
        expiry_seconds=url_expiry_seconds,
    ).url

    return BuildManifest(
        build_id=build_id,
        metadata=metadata,
        input_urls=input_urls,
        output_url=output_url,
        finalize_url=finalize_url,
    )
