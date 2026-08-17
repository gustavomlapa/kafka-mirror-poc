"""Google Cloud IAM OAuth Token Provider for Apache Kafka SASL/OAUTHBEARER authentication.

This module provides callbacks and token generators using Google Cloud Application
Default Credentials (ADC) for Python Kafka clients (such as confluent-kafka).
"""

import logging
import time
from typing import Optional, Tuple
import google.auth
import google.auth.transport.requests

logger = logging.getLogger("gcp_kafka_auth")

CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"

_global_token_provider: Optional["GCPTokenProvider"] = None


class GCPTokenProvider:
    """Manages acquisition and refresh of Google Cloud OAuth2 access tokens

    for SASL/OAUTHBEARER Kafka authentication.
    """

    def __init__(self, scopes: Optional[list] = None):
        self.scopes = scopes or [CLOUD_PLATFORM_SCOPE]
        self._credentials = None
        self._request = google.auth.transport.requests.Request()
        self._cached_token: Optional[str] = None
        self._cached_expiry: float = 0.0
        self._initialize_credentials()

    def _initialize_credentials(self):
        try:
            self._credentials, _ = google.auth.default(scopes=self.scopes)
            logger.info("Successfully loaded Google Application Default Credentials.")
        except Exception as exc:
            logger.warning(
                f"Warning loading default credentials with custom scopes: {exc}. Trying default fallback..."
            )
            try:
                self._credentials, _ = google.auth.default()
                logger.info("Loaded fallback Google Application Default Credentials.")
            except Exception as e2:
                logger.error(f"Failed to load Google Cloud credentials: {e2}")
                raise

    def get_token(self) -> Tuple[str, float]:
        """Fetches a valid GCP OAuth2 access token and its expiry timestamp.

        Returns:
            Tuple[str, float]: (access_token_string, expiry_epoch_seconds)
        """
        now = time.time()
        # Return cached token if valid for at least another 60 seconds
        if self._cached_token and self._cached_expiry > (now + 60):
            return self._cached_token, self._cached_expiry

        if not self._credentials:
            self._initialize_credentials()

        try:
            if not self._credentials.valid or not self._credentials.token:
                self._credentials.refresh(self._request)
                logger.info("Acquired fresh Google Cloud OAuth access token.")

            token = self._credentials.token
            if not token:
                raise ValueError("Credentials refreshed but token is empty.")

            if self._credentials.expiry:
                expiry_seconds = float(self._credentials.expiry.timestamp())
            else:
                expiry_seconds = now + 3600.0

            self._cached_token = token
            self._cached_expiry = expiry_seconds
            return token, expiry_seconds

        except Exception as exc:
            logger.error(f"Error fetching Google Cloud OAuth token: {exc}")
            # If we had a cached token, fallback to it to avoid breaking C thread
            if self._cached_token:
                logger.warning("Using existing cached token as fallback.")
                return self._cached_token, now + 300.0
            raise


def confluent_oauth_callback(*args, **kwargs) -> Tuple[str, float]:
    """Callback function compatible with confluent_kafka's oauth_cb configuration.

    Accepts any arguments passed by librdkafka (e.g. oauth_config string) and returns
    (token_str, expiry_time_in_seconds).
    """
    global _global_token_provider
    try:
        if _global_token_provider is None:
            _global_token_provider = GCPTokenProvider()
        return _global_token_provider.get_token()
    except Exception as e:
        logger.error(f"Exception inside confluent_oauth_callback: {e}")
        raise
