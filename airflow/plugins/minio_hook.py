"""
MinIO connection check hook for Airflow.

Checks TCP connectivity to the MinIO S3 API endpoint.
"""

from __future__ import annotations

import logging
import socket

logger = logging.getLogger(__name__)


class MinIOHook:
    """Simple connectivity check for MinIO."""

    def check_connection(self, host: str = "localhost", port: int = 9000) -> bool:
        """Check if MinIO endpoint is reachable via TCP socket.

        Returns True if connection succeeds, logs error and returns False otherwise.
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()

            if result == 0:
                logger.info("MinIO connection OK: %s:%s", host, port)
                return True
            else:
                logger.error(
                    "MinIO connection failed: %s:%s — error %d", host, port, result
                )
                return False
        except Exception as e:
            logger.error("MinIO connection error: %s:%s — %s", host, port, e)
            return False
