import csv
import re
import time
from abc import ABC
from datetime import datetime
from http import HTTPStatus
from io import BytesIO, TextIOWrapper
from typing import Any, Mapping, Optional, MutableMapping, Iterable, List
from urllib.parse import urljoin
from zipfile import ZipFile

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.exceptions import DefaultBackoffException, RequestBodyException
from airbyte_cdk.sources.streams.http.http import BODY_REQUEST_METHODS

DATE_FORMAT = "%Y-%m-%d"


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
        self.logger.info("Orders: %s", response.json().get("list").get("meta"))
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
            _end_date = pendulum.now("utc").strftime(DATE_FORMAT)

        params["returnLastModifiedEndDate"] = _end_date
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info("Returns: %s", response.json().get("meta"))
        if response.status_code == HTTPStatus.OK:
            if self.data_field:
                yield from response.json().get(self.data_field, [])
            else:
                yield from response.json()
            return


class Items(WalmartStream, ABC):
    """
    https://developer.walmart.com/api/us/mp/items#operation/getAllItems
    """
    primary_key = "sku"
    data_field = "ItemResponse"

    def path(self, **kwargs) -> str:
        return "items"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        next_page_token = response.json().get(self.next_page_token_field)
        if next_page_token:
            return {self.next_page_token_field: next_page_token}

    def request_params(self, next_page_token: Mapping[str, Any] = None, *args, **kvargs) -> MutableMapping[str, Any]:
        if next_page_token:
            return {
                self.next_page_token_field: next_page_token[self.next_page_token_field]
            }

        return {
            "limit": self.limit
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info("Items: %s, %s", response.json().get("totalItems"), response.json().get("nextCursor"))
        if response.status_code == HTTPStatus.OK:
            if self.data_field:
                yield from response.json().get(self.data_field, [])
            else:
                yield from response.json()
            return


class Inventories(WalmartStream, ABC):
    """
    https://developer.walmart.com/api/us/mp/inventory#operation/getMultiNodeInventoryForAllSkuAndAllShipNodes
    """
    primary_key = "sku"
    data_field = "inventories"
    limit = "50"

    def path(self, **kwargs) -> str:
        return "inventories"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        next_page_token = response.json().get("meta").get(self.next_page_token_field)
        if next_page_token:
            return {self.next_page_token_field: next_page_token}

    def request_params(self, next_page_token: Mapping[str, Any] = None, *args, **kvargs) -> MutableMapping[str, Any]:
        if next_page_token:
            return {
                self.next_page_token_field: next_page_token[self.next_page_token_field]
            }

        return {
            "limit": self.limit
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info("Inventories: %s", response.json().get("meta"))
        if response.status_code == HTTPStatus.OK:
            if self.data_field:
                yield from response.json().get("elements").get(self.data_field, [])
            else:
                yield from response.json()
            return


class WalmartOnRequestReportStream(WalmartStream, ABC):
    report_request_path = "reports/reportRequests"
    report_download_path = "reports/downloadReport"
    reportType = ""
    reportVersion = "v1"

    max_wait_seconds = 7200
    sleep_for_seconds = 300

    def path(self, **kwargs) -> str:
        return ""

    def request_headers(self, *args, **kvargs) -> MutableMapping[str, Any]:
        headers = super().request_headers()
        headers["Content-Type"] = "application/json"
        return headers

    def _send_request(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any] = None) -> requests.Response:
        response: requests.Response = self._session.send(request)
        if self.should_retry(response):
            raise DefaultBackoffException(request=request, response=response)
        else:
            response.raise_for_status()
        return response

    def _create_prepared_request(self, path: str, http_method: str = "GET", headers: Mapping = None, params: Mapping = None,
                                 json: Any = None, data: Any = None) -> requests.PreparedRequest:
        request_args = {
            "method": http_method,
            "url": urljoin(self.url_base, path),
            "headers": headers,
            "params": params
        }

        if http_method.upper() in BODY_REQUEST_METHODS:
            if json and data:
                raise RequestBodyException(
                    "At the same time only one of the 'request_body_data' and 'request_body_json' functions can return data"
                )
            elif json:
                request_args["json"] = json
            elif data:
                request_args["data"] = data

        return self._session.prepare_request(requests.Request(**request_args))

    def _report_create(self) -> Mapping[str, Any]:
        resp = self._send_request(
            self._create_prepared_request(
                http_method="POST",
                path=self.report_request_path,
                headers=dict(self.request_headers(), **self.authenticator.get_auth_header()),
                params={"reportType": self.reportType, "reportVersion": self.reportVersion}
            ))
        return resp.json()

    def _report_status(self, request_id: str) -> Mapping[str, Any]:
        resp = self._send_request(
            self._create_prepared_request(
                path=self.report_request_path + "/" + request_id,
                headers=dict(self.request_headers(), **self.authenticator.get_auth_header()),
            ))
        return resp.json()

    def _report_download(self, request_id: str) -> requests.Response:
        resp = self._send_request(
            self._create_prepared_request(
                path=self.report_download_path,
                headers=dict(self.request_headers(), **self.authenticator.get_auth_header()),
                params={"requestId": request_id}
            ))
        return resp

    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: List[str] = None,
            stream_slice: Mapping[str, Any] = None,
            stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        # create report
        create_payload = self._report_create()
        request_id = create_payload["requestId"]
        if not request_id:
            raise Exception(f"no request id received in payload: ", create_payload)

        # report status
        in_progress = True
        is_ready = False
        is_error = False
        start_time = pendulum.now("utc")
        seconds_waited = 0
        report_status_payload = {}

        while in_progress and seconds_waited < self.max_wait_seconds:
            report_status_payload = self._report_status(request_id=request_id)
            seconds_waited = (pendulum.now("utc") - start_time).seconds
            in_progress = report_status_payload.get("requestStatus") in ["RECEIVED", "INPROGRESS"]
            is_ready = report_status_payload.get("requestStatus") == "READY"
            is_error = report_status_payload.get("requestStatus") == "ERROR"
            if in_progress:
                self.logger.info(f"sleeping for {self.sleep_for_seconds} seconds")
                time.sleep(self.sleep_for_seconds)

        if is_ready:
            # report download
            resp = self._report_download(request_id=request_id)
            yield from self.parse_response(resp, stream_state, stream_slice)
        elif is_error:
            raise Exception(f"report status is ERROR")
        else:
            raise Exception(f"unknown response for stream `{self.name}`. Response body {report_status_payload}")

    def parse_response(
            self, response: requests.Response, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping]:
        # fetch zipped content from download url
        resp_content = requests.get(response.json().get("downloadURL")).content

        # unzip and read csv document
        zipped = ZipFile(BytesIO(resp_content))
        file = zipped.open(zipped.infolist()[0])
        lines = []

        data = str(file.read())
        rows = data.split("\\n")
        header = rows[0].lstrip("b'").split(",")
        p = re.compile('[a-zA-Z0-9]"",""[a-zA-Z0-9]')

        for i in range(1, len(rows)):
            current = rows[i]
            # find all regex matches
            match_list = p.findall(current)
            for source in match_list:
                target = str(source).replace(",", ";")
                current = current.replace(source, target)

            lines.append(dict(zip(header, current.split(","))))

        yield from lines


class ItemReport(WalmartOnRequestReportStream, ABC):
    """

    """
    primary_key = "SKU"
    reportType = "ITEM"
    reportVersion = "v2"


class AvailableDates(WalmartStream, ABC):

    @property
    def primary_key(self) -> str:
        return None

    def path(self, **kwargs):
        return "/v3/report/reconreport/availableReconFiles?reportVersion=v1"

    def parse_response(self, response: requests.Response, **kwargs):
        response_json = response.json()
        return response_json.get("availableApReportDates", [])


class ReconciliationReport(WalmartStream, ABC):
    @property
    def primary_key(self) -> str:
        return None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dates_stream = AvailableDates(**kwargs)

    def request_headers(self, *args, **kvargs) -> MutableMapping[str, Any]:
        return {
            "Accept": "application/octet-stream",
            "WM_SVC.NAME": "Walmart Marketplace",
            "WM_QOS.CORRELATION_ID": "b3261d2d-028a-4ef7-8602-633c23200af6"
        }

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs):
        report_date = stream_slice["report_date"]
        return f"/v3/report/reconreport/reconFile?reportDate={report_date}&reportVersion=v1"

    def stream_slices(self, **kwargs) -> Iterable[Mapping[str, Any]]:
        available_dates = self.dates_stream.read_records(sync_mode=SyncMode.full_refresh)
        start_date = datetime.strptime(self.start_date, DATE_FORMAT)
        end_date = datetime.strptime(self.end_date, DATE_FORMAT) if self.end_date else datetime.utcnow()

        filtered_dates = [date for date in available_dates if start_date <= datetime.strptime(date, '%m%d%Y') <= end_date]

        for report_date in filtered_dates:
            yield {"report_date": report_date}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        # unzip and read csv document
        zipped = ZipFile(BytesIO(response.content))
        file = zipped.open(zipped.infolist()[0])
        reader = csv.DictReader(TextIOWrapper(file))
        next(reader, None)  # skip the second line
        for row in reader:
            yield row
