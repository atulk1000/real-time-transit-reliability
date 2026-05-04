from __future__ import annotations

from typing import Any

import requests


class WmataClient:
    def __init__(self, base_url: str, key_header_name: str, verify_ssl: bool = True) -> None:
        self.base_url = base_url.rstrip("/")
        self.key_header_name = key_header_name
        self.verify_ssl = verify_ssl

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        api_key: str | None = None,
    ) -> requests.Response:
        headers = {}
        if api_key:
            headers[self.key_header_name] = api_key
        return requests.get(
            f"{self.base_url}{endpoint}",
            params=params or {},
            headers=headers,
            verify=self.verify_ssl,
            timeout=20,
        )
