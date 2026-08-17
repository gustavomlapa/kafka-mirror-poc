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


class GCPTokenProvider:
    """Manages acquisition and refresh of Google Cloud OAuth2 access tokens

    for SASL/OAUTHBEARER Kafka authentication.
    """

    def __init__(self, scopes: Optional[list] = None):
        self.scopes = scopes or [CLOUD_PLATFORM_SCOPE]
        self._credentials = None
        self._request = google.auth.transport.requests.Request()
        self._initialize_credentials()

    def _initialize_credentials(self):
        try:
            self._credentials, _ = google.auth.default(scopes=self.scopes)
            logger.info("Successfully loaded Google Application Default Credentials.")
        except Exception as exc:
            logger.error(
                f"Failed to load Google Cloud credentials: {exc}. "
                "Ensure GOOGLE_APPLICATION_CREDENTIALS or gcloud ADC is configured."
            )
            raise

    def get_token(self) -> Tuple[str, float]:
        """Fetches a valid GCP OAuth2 access token and its expiry timestamp.

        Returns:
            Tuple[str, float]: (access_token_string, expiry_epoch_seconds)
        """
        if not self._credentials:
            self._initialize_credentials()

        # Refresh if token is missing or near expiration
        if not self._credentials.valid:
            try:
                self._credentials.refresh(self._request)
                logger.debug("Refreshed Google Cloud OAuth access token.")
            except Exception as exc:
                logger.error(f"Error refreshing Google Cloud credentials: {exc}")
                raise

        token = self._credentials.token
        # Calculate expiry epoch time in seconds
        if self._credentials.expiry:
            expiry_seconds = self._credentials.expiry.timestamp()
        else:
            # Default to 3600 seconds from now if expiry timestamp not directly exposed
            expiry_seconds = time.time() + 3600

        return token, expiry_seconds


def confluent_oauth_callback(config_str: Optional[str] = None) -> Tuple[str, float]:
    """Callback function compatible with confluent_kafka's oauth_cb configuration.

    Usage with confluent_kafka:
        conf = {
            'bootstrap.servers': '...',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'OAUTHBEARER',
            'oauth_cb': confluent_oauth_callback,
        }
    """
    provider = GCPTokenProvider()
    token, expiry = provider.get_token()
    return token, expiry
