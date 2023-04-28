#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_walmart_seller import SourceWalmartSeller

if __name__ == "__main__":
    source = SourceWalmartSeller()
    launch(source, sys.argv[1:])
