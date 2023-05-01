from abc import ABC
from http import HTTPStatus
from typing import Any, Mapping, Optional, MutableMapping, Iterable

import pendulum
import requests
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams.http import HttpStream

DATE_TIME_FORMAT = "%Y-%m-%d"


class WalmartStream(HttpStream, ABC):
    url = "https://marketplace.walmartapis.com/v3/"
    data_field = ""
    limit = "200"
    next_page_token_field = "nextCursor"

    def __init__(self, start_date: str, end_date: str, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.start_date = start_date
        self.end_date = end_date

    @property
    def url_base(self):
        return self.url

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_headers(self, *args, **kvargs) -> MutableMapping[str, Any]:
        return {
            "Accept": "application/json",
            "WM_SVC.NAME": "Walmart Marketplace",
            "WM_QOS.CORRELATION_ID": "b3261d2d-028a-4ef7-8602-633c23200af6"
        }

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def _params_from_next_page_token(self, token_val: str):
        params = {}
        qps = token_val.split("&")
        for qp in qps:
            qp = qp.strip("?")
            p = qp.split("=")
            if len(p) == 2:
                params[p[0]] = p[1]

        return params


class Orders(WalmartStream, ABC):
    """
    https://developer.walmart.com/api/us/mp/orders#operation/getAllOrders
    """

    primary_key = "purchaseOrderId"
    data_field = "order"

    def path(self, **kwargs) -> str:
        return "orders"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        next_page_token = response.json().get("list").get("meta").get(self.next_page_token_field)
        if next_page_token:
            return {self.next_page_token_field: next_page_token}

    def request_params(self, next_page_token: Mapping[str, Any] = None, *args, **kvargs) -> MutableMapping[str, Any]:
        if next_page_token:
            return self._params_from_next_page_token(str(next_page_token[self.next_page_token_field]))

        params = {
            "lastModifiedStartDate": self.start_date,
            "limit": self.limit
        }

        if self.end_date:
            params["lastModifiedEndDate"] = self.end_date

        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        print(response.json().get("list").get("meta"))
        if response.status_code == HTTPStatus.OK:
            if self.data_field:
                yield from response.json().get("list").get("elements").get(self.data_field, [])
            else:
                yield from response.json()
            return


class Returns(WalmartStream, ABC):
    """
    https://developer.walmart.com/api/us/mp/returns#operation/getReturns
    """

    primary_key = "returnOrderId"
    data_field = "returnOrders"

    def path(self, **kwargs) -> str:
        return "returns"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        next_page_token = response.json().get("meta").get(self.next_page_token_field)
        if next_page_token:
            return {self.next_page_token_field: next_page_token}

    def request_params(self, next_page_token: Mapping[str, Any] = None, *args, **kvargs) -> MutableMapping[str, Any]:
        if next_page_token:
            return self._params_from_next_page_token(str(next_page_token[self.next_page_token_field]))

        params = {
            "returnLastModifiedStartDate": self.start_date,
            "limit": self.limit
        }
        _end_date = self.end_date

        if not _end_date:
            _end_date = pendulum.now("utc").strftime(DATE_TIME_FORMAT)

        params["returnLastModifiedEndDate"] = _end_date
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        print(response.json().get("meta"))
        if response.status_code == HTTPStatus.OK:
            if self.data_field:
                yield from response.json().get(self.data_field, [])
            else:
                yield from response.json()
            return


class Items(WalmartStream, ABC):
    primary_key = "sku"
    data_field = "ItemResponse"

    def path(self, **kwargs) -> str:
        return "items"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        print("items: ", response)
        if response.status_code == HTTPStatus.OK:
            if self.data_field:
                yield from response.json().get(self.data_field, [])
            else:
                yield from response.json()
            return
