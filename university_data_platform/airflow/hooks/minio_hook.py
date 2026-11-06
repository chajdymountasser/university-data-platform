"""
MinIO Airflow hook — wraps MinIOClient as a standard Airflow BaseHook.

Used internally by ingestion operators. Clients can also use it directly:

    from university_data_platform.airflow.hooks.minio_hook import MinIOHook

    with MinIOHook() as hook:
        hook.client.create_buckets()
"""

from __future__ import annotations

from typing import Optional

from airflow.hooks.base import BaseHook

from university_data_platform.storage.minio_client import MinIOClient


class MinIOHook(BaseHook):
    """Airflow hook that provides a configured MinIOClient.

    Reads connection parameters from environment variables
    (MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_USE_SSL)
    or from an Airflow connection ID.
    """

    conn_name_attr = "minio_conn_id"
    default_conn_name = "minio_default"
    conn_type = "minio"
    hook_name = "MinIO S3"

    def __init__(self, conn_id: str = "minio_default"):
        super().__init__()
        self.conn_id = conn_id
        self._client: Optional[MinIOClient] = None

    @property
    def client(self) -> MinIOClient:
        if self._client is None:
            self._client = MinIOClient()
        return self._client

    def __enter__(self) -> "MinIOHook":
        return self

    def __exit__(self, *args):
        pass

    def test_connection(self) -> tuple[bool, str]:
        """Test connectivity — used by Airflow connection UI."""
        try:
            buckets = self.client.list_buckets()
            return True, f"Connected. Buckets: {len(buckets)}"
        except Exception as e:
            return False, str(e)
