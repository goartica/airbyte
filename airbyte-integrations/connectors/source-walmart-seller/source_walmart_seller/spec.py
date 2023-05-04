#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from pydantic import BaseModel, Field


class WalmartSellerConfig(BaseModel):
    class Config:
        title = "Walmart Seller Spec"
        schema_extra = {"additionalProperties": True}

    client_id: str = Field(
        description="Walmart Client ID.",
        title="Walmart Client Id",
        order=1,
    )

    client_secret: str = Field(
        description="Walmart Client Secret.",
        title="Walmart Client Secret",
        airbyte_secret=True,
        order=2,
    )

    start_date: str = Field(
        description="UTC date and time in the format 2017-01-25. Any data before this date will not be replicated.",
        title="Start Date",
        pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
        examples=["2017-01-25"],
        order=3,
    )

    end_date: str = Field(
        None,
        description="UTC date and time in the format 2017-01-25T00:00:00Z. Any data after this date will not be replicated.",
        title="End Date",
        pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}$|^$",
        examples=["2017-01-25"],
        order=4,
    )
