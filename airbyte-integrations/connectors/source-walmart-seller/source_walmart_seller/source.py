#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import logging
from typing import Any, Mapping, Tuple, Optional, List

from airbyte_cdk.models import (
    SyncMode,
)
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .auth import WalmartAuthenticator
from .spec import WalmartSellerConfig
from .streams import Orders, Items, Returns, Inventories


class SourceWalmartSeller(AbstractSource):
    def _get_stream_kwargs(self, config: WalmartSellerConfig) -> Mapping[str, Any]:
        auth = WalmartAuthenticator(
            client_id=config.client_id,
            client_secret=config.client_secret
        )

        stream_kwargs = {
            "authenticator": auth,
            "start_date": config.start_date,
            "end_date": config.end_date,
        }

        return stream_kwargs

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        """
        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            config = WalmartSellerConfig.parse_obj(config)
            stream_args = self._get_stream_kwargs(config)

            orders_stream = Orders(**stream_args)
            for record in orders_stream.read_records(sync_mode=SyncMode.full_refresh):
                print(record)
        except Exception as e:
            return False, str(e)

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        :return list of streams for current source
        """
        config = WalmartSellerConfig.parse_obj(config)
        stream_args = self._get_stream_kwargs(config)
        return [
            Orders(**stream_args),
            Items(**stream_args),
            Returns(**stream_args),
            Inventories(**stream_args),
        ]
