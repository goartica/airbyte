import base64
from typing import Any, Mapping, Tuple

import pendulum
import requests
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator


class WalmartAuthenticator(HttpAuthenticator):
    _token_url = "https://marketplace.walmartapis.com/v3/token"
    _auth_header = "Authorization"
    _auth_method = "Basic"

    def __init__(self, client_id: str, client_secret: str):
        self._auth_token = base64.b64encode(f"{client_id}:{client_secret}".encode("utf8")).decode("utf8")
        self._token_expiry_date = pendulum.now().subtract(days=1)
        self._access_token = None

    def _get_access_token(self) -> str:
        if self._token_has_expired():
            t0 = pendulum.now()
            token, expires_in = self._generate_access_token()
            self._access_token = token
            self._token_expiry_date = t0.add(seconds=expires_in)

        return self._access_token

    def _generate_access_token(self) -> Tuple[str, int]:
        """
        returns a tuple of (access_token, token_lifespan_in_seconds)
        """
        try:
            response = requests.request(
                method="POST",
                url=self._token_url,
                data={"grant_type": "client_credentials"},
                headers={
                    self._auth_header: f"{self._auth_method} {self._auth_token}",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                    "WM_SVC.NAME": "Walmart Marketplace",
                    "WM_QOS.CORRELATION_ID": "b3261d2d-028a-4ef7-8602-633c23200af6"
                },
            )
            response.raise_for_status()
            response_json = response.json()
            return response_json["access_token"], int(response_json["expires_in"])
        except Exception as e:
            raise Exception(f"Error while generating access token: {e}") from e

    def _token_has_expired(self) -> bool:
        return pendulum.now() > self._token_expiry_date

    def get_auth_header(self) -> Mapping[str, Any]:
        return {"WM_SEC.ACCESS_TOKEN": f"{self._get_access_token()}"}
