"""Tests for cache warming API."""

import asyncio
import time

import pytest


class TestCacheWarmer:
    """Tests for CacheWarmer class."""

    def test_warming_job_creation(self):
        """Test creating a warming job."""
        from strata.cache_warmer import WarmingJob
        from strata.types import WarmAsyncRequest, WarmJobStatus

        request = WarmAsyncRequest(
            tables=["file:///warehouse#ns.table1", "file:///warehouse#ns.table2"],
            columns=["id", "name"],
            concurrent=4,
            priority=1,
        )

        job = WarmingJob(
            job_id="test-123",
            request=request,
            tables_total=2,
        )

        assert job.job_id == "test-123"
        assert job.status == WarmJobStatus.PENDING
        assert job.tables_total == 2
        assert job.tables_completed == 0

    def test_warming_job_to_progress(self):
        """Test converting job to progress response."""
        from strata.cache_warmer import WarmingJob
        from strata.types import WarmAsyncRequest, WarmJobStatus

        request = WarmAsyncRequest(tables=["table1"])
        job = WarmingJob(
            job_id="test-456",
            request=request,
            status=WarmJobStatus.RUNNING,
            tables_total=1,
            tables_completed=0,
            row_groups_total=10,
            row_groups_completed=5,
            row_groups_cached=3,
            row_groups_skipped=2,
            bytes_written=1024,
            started_at=time.time() - 1.0,
            current_table="table1",
        )

        progress = job.to_progress()

        assert progress.job_id == "test-456"
        assert progress.status == WarmJobStatus.RUNNING
        assert progress.tables_total == 1
        assert progress.row_groups_total == 10
        assert progress.row_groups_completed == 5
        assert progress.row_groups_cached == 3
        assert progress.row_groups_skipped == 2
        assert progress.bytes_written == 1024
        assert progress.current_table == "table1"
        assert progress.elapsed_ms >= 1000  # At least 1 second


class TestCacheWarmerIntegration:
    """Integration tests for cache warmer with server."""

    @pytest.mark.asyncio
    async def test_async_warm_endpoint(self, tmp_path):
        """Test POST /v1/cache/warm/async endpoint."""
        from httpx import ASGITransport, AsyncClient

        import strata.server as server_module
        from strata.cache_warmer import CacheWarmer
        from strata.config import StrataConfig
        from strata.pool_metrics import reset_metrics
        from strata.server import ServerState, app

        reset_metrics()
        config = StrataConfig(cache_dir=str(tmp_path))
        server_module._state = ServerState(config)

        # Initialize cache warmer
        server_module._state._cache_warmer = CacheWarmer(
            planner=server_module._state.planner,
            fetcher=server_module._state.fetcher,
            metrics=server_module._state.metrics,
        )
        await server_module._state._cache_warmer.start()

        try:
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                # Start async warming job (will fail since no real tables)
                response = await client.post(
                    "/v1/cache/warm/async",
                    json={
                        "tables": ["file:///nonexistent#ns.table"],
                        "concurrent": 2,
                    },
                )
                assert response.status_code == 200

                data = response.json()
                assert "job_id" in data
                assert data["status"] == "pending"
                assert data["tables_count"] == 1

                job_id = data["job_id"]

                # Wait a bit for job to process
                await asyncio.sleep(0.1)

                # Get job status
                response = await client.get(f"/v1/cache/warm/jobs/{job_id}")
                assert response.status_code == 200

                progress = response.json()
                assert progress["job_id"] == job_id
                # Job should be completed or failed (table doesn't exist)
                assert progress["status"] in ["running", "completed", "failed"]

        finally:
            await server_module._state._cache_warmer.stop()
            server_module._state._planning_executor.shutdown(wait=False)
            server_module._state._fetch_executor.shutdown(wait=False)
            server_module._state = None

    @pytest.mark.asyncio
    async def test_list_jobs_endpoint(self, tmp_path):
        """Test GET /v1/cache/warm/jobs endpoint."""
        from httpx import ASGITransport, AsyncClient

        import strata.server as server_module
        from strata.cache_warmer import CacheWarmer
        from strata.config import StrataConfig
        from strata.pool_metrics import reset_metrics
        from strata.server import ServerState, app

        reset_metrics()
        config = StrataConfig(cache_dir=str(tmp_path))
        server_module._state = ServerState(config)
        server_module._state._cache_warmer = CacheWarmer(
            planner=server_module._state.planner,
            fetcher=server_module._state.fetcher,
            metrics=server_module._state.metrics,
        )
        await server_module._state._cache_warmer.start()

        try:
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                # List jobs (should be empty initially)
                response = await client.get("/v1/cache/warm/jobs")
                assert response.status_code == 200
                assert response.json()["jobs"] == []

                # Start a job
                await client.post(
                    "/v1/cache/warm/async",
                    json={"tables": ["table1"]},
                )

                # List jobs including completed
                await asyncio.sleep(0.1)
                response = await client.get(
                    "/v1/cache/warm/jobs?include_completed=true"
                )
                assert response.status_code == 200
                jobs = response.json()["jobs"]
                assert len(jobs) >= 1

        finally:
            await server_module._state._cache_warmer.stop()
            server_module._state._planning_executor.shutdown(wait=False)
            server_module._state._fetch_executor.shutdown(wait=False)
            server_module._state = None

    @pytest.mark.asyncio
    async def test_cancel_job_endpoint(self, tmp_path):
        """Test DELETE /v1/cache/warm/jobs/{job_id} endpoint."""
        from httpx import ASGITransport, AsyncClient

        import strata.server as server_module
        from strata.cache_warmer import CacheWarmer
        from strata.config import StrataConfig
        from strata.pool_metrics import reset_metrics
        from strata.server import ServerState, app

        reset_metrics()
        config = StrataConfig(cache_dir=str(tmp_path))
        server_module._state = ServerState(config)
        server_module._state._cache_warmer = CacheWarmer(
            planner=server_module._state.planner,
            fetcher=server_module._state.fetcher,
            metrics=server_module._state.metrics,
        )
        await server_module._state._cache_warmer.start()

        try:
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                # Cancel nonexistent job
                response = await client.delete("/v1/cache/warm/jobs/nonexistent")
                assert response.status_code == 404

        finally:
            await server_module._state._cache_warmer.stop()
            server_module._state._planning_executor.shutdown(wait=False)
            server_module._state._fetch_executor.shutdown(wait=False)
            server_module._state = None

    @pytest.mark.asyncio
    async def test_job_not_found(self, tmp_path):
        """Test 404 for nonexistent job."""
        from httpx import ASGITransport, AsyncClient

        import strata.server as server_module
        from strata.cache_warmer import CacheWarmer
        from strata.config import StrataConfig
        from strata.pool_metrics import reset_metrics
        from strata.server import ServerState, app

        reset_metrics()
        config = StrataConfig(cache_dir=str(tmp_path))
        server_module._state = ServerState(config)
        server_module._state._cache_warmer = CacheWarmer(
            planner=server_module._state.planner,
            fetcher=server_module._state.fetcher,
            metrics=server_module._state.metrics,
        )
        await server_module._state._cache_warmer.start()

        try:
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get("/v1/cache/warm/jobs/nonexistent-id")
                assert response.status_code == 404

        finally:
            await server_module._state._cache_warmer.stop()
            server_module._state._planning_executor.shutdown(wait=False)
            server_module._state._fetch_executor.shutdown(wait=False)
            server_module._state = None


class TestWarmTypes:
    """Tests for warming request/response types."""

    def test_warm_async_request(self):
        """Test WarmAsyncRequest model."""
        from strata.types import WarmAsyncRequest

        request = WarmAsyncRequest(
            tables=["table1", "table2"],
            columns=["id", "name"],
            snapshot_id=12345,
            max_row_groups=100,
            concurrent=8,
            priority=5,
        )

        assert request.tables == ["table1", "table2"]
        assert request.columns == ["id", "name"]
        assert request.snapshot_id == 12345
        assert request.max_row_groups == 100
        assert request.concurrent == 8
        assert request.priority == 5

    def test_warm_async_request_defaults(self):
        """Test WarmAsyncRequest default values."""
        from strata.types import WarmAsyncRequest

        request = WarmAsyncRequest(tables=["table1"])

        assert request.columns is None
        assert request.snapshot_id is None
        assert request.max_row_groups is None
        assert request.concurrent == 4
        assert request.priority == 0

    def test_warm_job_status_enum(self):
        """Test WarmJobStatus enum values."""
        from strata.types import WarmJobStatus

        assert WarmJobStatus.PENDING.value == "pending"
        assert WarmJobStatus.RUNNING.value == "running"
        assert WarmJobStatus.COMPLETED.value == "completed"
        assert WarmJobStatus.FAILED.value == "failed"
        assert WarmJobStatus.CANCELLED.value == "cancelled"

    def test_warm_job_progress_model(self):
        """Test WarmJobProgress model."""
        from strata.types import WarmJobProgress, WarmJobStatus

        progress = WarmJobProgress(
            job_id="test-123",
            status=WarmJobStatus.RUNNING,
            tables_total=5,
            tables_completed=2,
            row_groups_total=100,
            row_groups_completed=40,
            row_groups_cached=30,
            row_groups_skipped=10,
            bytes_written=1024 * 1024,
            started_at=1234567890.0,
            completed_at=None,
            elapsed_ms=5000.0,
            current_table="ns.table3",
            errors=[],
        )

        assert progress.job_id == "test-123"
        assert progress.status == WarmJobStatus.RUNNING
        assert progress.tables_total == 5
        assert progress.tables_completed == 2
        assert progress.row_groups_completed == 40
        assert progress.bytes_written == 1024 * 1024
        assert progress.current_table == "ns.table3"

    def test_warm_async_response_model(self):
        """Test WarmAsyncResponse model."""
        from strata.types import WarmAsyncResponse, WarmJobStatus

        response = WarmAsyncResponse(
            job_id="abc123",
            status=WarmJobStatus.PENDING,
            tables_count=3,
            message="Job started",
        )

        assert response.job_id == "abc123"
        assert response.status == WarmJobStatus.PENDING
        assert response.tables_count == 3
        assert response.message == "Job started"
