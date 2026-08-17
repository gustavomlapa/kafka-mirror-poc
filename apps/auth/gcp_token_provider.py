"""Google Cloud IAM OAuth Token Provider for Managed Service for Apache Kafka.

Constructs the specialized 3-part JWT format required by Google Managed Kafka:
  Header:  {"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"}
  Payload: {"exp": <expiry>, "iat": <now>, "iss": "Google", "sub": "<principal/service_account>"}
  Token:   <google_access_token>
"""

import base64
import datetime
import json
import logging
import os
import time
from typing import Optional, Tuple

import google.auth
import google.auth.transport.requests

logger = logging.getLogger("gcp_kafka_auth")

CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def urlsafe_b64encode_nopad(source: str) -> str:
    """Base64 URL encodes string without trailing '=' padding."""
    return base64.urlsafe_b64encode(source.encode("utf-8")).decode("utf-8").rstrip("=")


class GCPTokenProvider:
    """Generates the GOOG_OAUTH2_TOKEN formatted bearer token for Google Managed Kafka."""

    HEADER = json.dumps({"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"})

    def __init__(self, principal: Optional[str] = None):
        self.principal = principal or os.getenv("GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL") or os.getenv("SERVICE_ACCOUNT_EMAIL")
        self._credentials = None
        self._request = google.auth.transport.requests.Request()
        self._cached_jwt: Optional[str] = None
        self._cached_expiry: float = 0.0
        self._initialize()

    def _initialize(self):
        try:
            self._credentials, _ = google.auth.default(scopes=[CLOUD_PLATFORM_SCOPE])
            logger.info("Loaded Google Application Default Credentials.")
        except Exception as exc:
            logger.warning(f"Default auth loading warning: {exc}. Retrying without scopes...")
            self._credentials, _ = google.auth.default()

    def _get_principal(self) -> str:
        if self.principal:
            return self.principal
        # Try finding email from credentials
        email = getattr(self._credentials, "service_account_email", None)
        if email and email != "default":
            return email
        # Fallback to service account from env or generic placeholder
        return os.getenv("SERVICE_ACCOUNT_EMAIL", "sa-kafka-poc@poc-kafka-mirror.iam.gserviceaccount.com")

    def _get_valid_credentials(self):
        if not self._credentials:
            self._initialize()
        if not self._credentials.valid or not self._credentials.token:
            self._credentials.refresh(self._request)
        return self._credentials

    def get_token(self, *args, **kwargs) -> Tuple[str, float]:
        """Returns the encoded (GOOG_OAUTH2_TOKEN_JWT, expiry_epoch_seconds) tuple."""
        now = time.time()
        # Return cached token if still valid for 60 seconds
        if self._cached_jwt and self._cached_expiry > (now + 60):
            return self._cached_jwt, self._cached_expiry

        try:
            creds = self._get_valid_credentials()

            if creds.expiry:
                if creds.expiry.tzinfo is None:
                    exp_utc = creds.expiry.replace(tzinfo=datetime.timezone.utc)
                else:
                    exp_utc = creds.expiry
                expiry_seconds = float(exp_utc.timestamp())
            else:
                expiry_seconds = now + 3600.0

            sub = self._get_principal()
            now_dt = datetime.datetime.now(datetime.timezone.utc)

            payload_dict = {
                "exp": int(expiry_seconds),
                "iat": int(now_dt.timestamp()),
                "iss": "Google",
                "sub": sub,
            }
            payload_json = json.dumps(payload_dict)

            # Construct 3-part GOOG_OAUTH2_TOKEN string:
            # header.payload.google_access_token
            formatted_token = ".".join([
                urlsafe_b64encode_nopad(self.HEADER),
                urlsafe_b64encode_nopad(payload_json),
                urlsafe_b64encode_nopad(creds.token),
            ])

            self._cached_jwt = formatted_token
            self._cached_expiry = expiry_seconds
            logger.info(f"Generated GOOG_OAUTH2_TOKEN for principal: {sub}")
            return formatted_token, expiry_seconds

        except Exception as exc:
            logger.error(f"Error creating Kafka GOOG_OAUTH2_TOKEN: {exc}")
            if self._cached_jwt:
                return self._cached_jwt, now + 120.0
            raise


_global_provider = GCPTokenProvider()


def confluent_oauth_callback(*args, **kwargs) -> Tuple[str, float]:
    """Callback method for confluent-kafka oauth_cb."""
    global _global_provider
    return _global_provider.get_token(*args, **kwargs)
