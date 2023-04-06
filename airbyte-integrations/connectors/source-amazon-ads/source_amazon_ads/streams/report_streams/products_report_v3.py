#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from http import HTTPStatus
from typing import List

from .report_streams import ReportStream, ReportInfo

METRICS_MAP = {
    "campaigns": [
        "impressions",
        "clicks",
        "cost",
        "purchases1d",
        "purchases7d",
        "purchases14d",
        "purchases30d",
        "purchasesSameSku1d",
        "purchasesSameSku7d",
        "purchasesSameSku14d",
        "purchasesSameSku30d",
        "unitsSoldClicks1d",
        "unitsSoldClicks7d",
        "unitsSoldClicks14d",
        "unitsSoldClicks30d",
        "sales1d",
        "sales7d",
        "sales14d",
        "sales30d",
        "attributedSalesSameSku1d",
        "attributedSalesSameSku7d",
        "attributedSalesSameSku14d",
        "attributedSalesSameSku30d",
        "unitsSoldSameSku1d",
        "unitsSoldSameSku7d",
        "unitsSoldSameSku14d",
        "unitsSoldSameSku30d",
        "kindleEditionNormalizedPagesRead14d",
        "kindleEditionNormalizedPagesRoyalties14d",
        "startDate",
        "endDate",
        "campaignBiddingStrategy",
        "costPerClick",
        "clickThroughRate",
        "spend",
        "campaignName",
        "campaignId",
        "campaignStatus",
        "campaignBudgetType",
        "campaignBudgetAmount",
        "campaignRuleBasedBudgetAmount",
        "campaignApplicableBudgetRuleId",
        "campaignApplicableBudgetRuleName",
        "campaignBudgetCurrencyCode"
    ],
    "adGroups": [
        "campaignName",
        "campaignId",
        "adGroupName",
        "adGroupId",
        "impressions",
        "clicks",
        "cost",
        "purchases1d",
        "purchases7d",
        "purchases14d",
        "purchases30d",
        "purchasesSameSku1d",
        "purchasesSameSku7d",
        "purchasesSameSku14d",
        "purchasesSameSku30d",
        "unitsSoldClicks1d",
        "unitsSoldClicks7d",
        "unitsSoldClicks14d",
        "unitsSoldClicks30d",
        "sales1d",
        "sales7d",
        "sales14d",
        "sales30d",
        "attributedSalesSameSku1d",
        "attributedSalesSameSku7d",
        "attributedSalesSameSku14d",
        "attributedSalesSameSku30d",
        "unitsSoldSameSku1d",
        "unitsSoldSameSku7d",
        "unitsSoldSameSku14d",
        "unitsSoldSameSku30d",
    ],
    "keywords": [
        "impressions",
        "clicks",
        "costPerClick",
        "clickThroughRate",
        "cost",
        "purchases1d",
        "purchases7d",
        "purchases14d",
        "purchases30d",
        "purchasesSameSku1d",
        "purchasesSameSku7d",
        "purchasesSameSku14d",
        "purchasesSameSku30d",
        "unitsSoldClicks1d",
        "unitsSoldClicks7d",
        "unitsSoldClicks14d",
        "unitsSoldClicks30d",
        "sales1d",
        "sales7d",
        "sales14d",
        "sales30d",
        "attributedSalesSameSku1d",
        "attributedSalesSameSku7d",
        "attributedSalesSameSku14d",
        "attributedSalesSameSku30d",
        "unitsSoldSameSku1d",
        "unitsSoldSameSku7d",
        "unitsSoldSameSku14d",
        "unitsSoldSameSku30d",
        "kindleEditionNormalizedPagesRead14d",
        "kindleEditionNormalizedPagesRoyalties14d",
        "salesOtherSku7d",
        "unitsSoldOtherSku7d",
        "acosClicks7d",
        "acosClicks14d",
        "roasClicks7d",
        "roasClicks14d",
        "keywordId",
        "keyword",
        "campaignBudgetCurrencyCode",
        "startDate",
        "endDate",
        "portfolioId",
        "campaignName",
        "campaignId",
        "campaignBudgetType",
        "campaignBudgetAmount",
        "campaignStatus",
        "keywordBid",
        "adGroupName",
        "adGroupId",
        "keywordType",
        "matchType",
        "targeting",
        "adKeywordStatus",
    ],
    "targets": [
        "campaignName",
        "campaignId",
        "adGroupName",
        "adGroupId",
        "keywordId",
        "keyword",
        "targeting",
        "keywordType",
        "impressions",
        "clicks",
        "cost",
        "purchases1d",
        "purchases7d",
        "purchases14d",
        "purchases30d",
        "purchasesSameSku1d",
        "purchasesSameSku7d",
        "purchasesSameSku14d",
        "purchasesSameSku30d",
        "unitsSoldClicks1d",
        "unitsSoldClicks7d",
        "unitsSoldClicks14d",
        "unitsSoldClicks30d",
        "sales1d",
        "sales7d",
        "sales14d",
        "sales30d",
        "attributedSalesSameSku1d",
        "attributedSalesSameSku7d",
        "attributedSalesSameSku14d",
        "attributedSalesSameSku30d",
        "unitsSoldSameSku1d",
        "unitsSoldSameSku7d",
        "unitsSoldSameSku14d",
        "unitsSoldSameSku30d",
    ],
    "productAds": [
        "campaignName",
        "campaignId",
        "adGroupName",
        "adGroupId",
        "adId",
        "impressions",
        "clicks",
        "cost",
        "campaignBudgetCurrencyCode",
        "advertisedAsin",
        "purchases1d",
        "purchases7d",
        "purchases14d",
        "purchases30d",
        "purchasesSameSku1d",
        "purchasesSameSku7d",
        "purchasesSameSku14d",
        "purchasesSameSku30d",
        "unitsSoldClicks1d",
        "unitsSoldClicks7d",
        "unitsSoldClicks14d",
        "unitsSoldClicks30d",
        "sales1d",
        "sales7d",
        "sales14d",
        "sales30d",
        "attributedSalesSameSku1d",
        "attributedSalesSameSku7d",
        "attributedSalesSameSku14d",
        "attributedSalesSameSku30d",
        "unitsSoldSameSku1d",
        "unitsSoldSameSku7d",
        "unitsSoldSameSku14d",
        "unitsSoldSameSku30d",
    ],
    "asins_keywords": [
        "campaignName",
        "campaignId",
        "adGroupName",
        "adGroupId",
        "keywordId",
        "keyword",
        "advertisedAsin",
        "purchasedAsin",
        "advertisedSku",
        "campaignBudgetCurrencyCode",
        "matchType",
        "unitsSoldClicks1d",
        "unitsSoldClicks7d",
        "unitsSoldClicks14d",
        "unitsSoldClicks30d",
        "unitsSoldOtherSku1d",
        "unitsSoldOtherSku7d",
        "unitsSoldOtherSku14d",
        "unitsSoldOtherSku30d",
        "salesOtherSku1d",
        "salesOtherSku7d",
        "salesOtherSku14d",
        "salesOtherSku30d",
    ],
    "asins_targets": [
        "campaignName",
        "campaignId",
        "adGroupName",
        "adGroupId",
        "advertisedAsin",
        "purchasedAsin",
        "advertisedSku",
        "campaignBudgetCurrencyCode",
        "matchType",
        "unitsSoldClicks1d",
        "unitsSoldClicks7d",
        "unitsSoldClicks14d",
        "unitsSoldClicks30d",
        "unitsSoldOtherSku1d",
        "unitsSoldOtherSku7d",
        "unitsSoldOtherSku14d",
        "unitsSoldOtherSku30d",
        "salesOtherSku1d",
        "salesOtherSku7d",
        "salesOtherSku14d",
        "salesOtherSku30d",
        "keywordId",
        "targeting",
        "keywordType",
    ],
    "searchTerm": [
        "impressions",
        "clicks",
        "costPerClick",
        "clickThroughRate",
        "cost",
        "purchases1d",
        "purchases7d",
        "purchases14d",
        "purchases30d",
        "purchasesSameSku1d",
        "purchasesSameSku7d",
        "purchasesSameSku14d",
        "purchasesSameSku30d",
        "unitsSoldClicks1d",
        "unitsSoldClicks7d",
        "unitsSoldClicks14d",
        "unitsSoldClicks30d",
        "sales1d",
        "sales7d",
        "sales14d",
        "sales30d",
        "attributedSalesSameSku1d",
        "attributedSalesSameSku7d",
        "attributedSalesSameSku14d",
        "attributedSalesSameSku30d",
        "unitsSoldSameSku1d",
        "unitsSoldSameSku7d",
        "unitsSoldSameSku14d",
        "unitsSoldSameSku30d",
        "kindleEditionNormalizedPagesRead14d",
        "kindleEditionNormalizedPagesRoyalties14d",
        "salesOtherSku7d",
        "unitsSoldOtherSku7d",
        "acosClicks7d",
        "acosClicks14d",
        "roasClicks7d",
        "roasClicks14d",
        "keywordId",
        "keyword",
        "campaignBudgetCurrencyCode",
        "startDate",
        "endDate",
        "portfolioId",
        "searchTerm",
        "campaignName",
        "campaignId",
        "campaignBudgetType",
        "campaignBudgetAmount",
        "campaignStatus",
        "keywordBid",
        "adGroupName",
        "adGroupId",
        "keywordType",
        "matchType",
        "targeting",
        "adKeywordStatus",
    ]
}


class SponsoredProductsReportStreamV3(ReportStream):
    """
    https://advertising.amazon.com/API/docs/en-us/sponsored-products/2-0/openapi#/Reports
    https://advertising.amazon.com/API/docs/en-us/reporting/v3/migration-guide
    https://advertising.amazon.com/API/docs/en-us/reporting/v3/report-types#sponsored-products
    """

    API_VERSION = "reporting"  # v3
    REPORT_DATE_FORMAT = "YYYY-MM-DD"
    ad_product = "SPONSORED_PRODUCTS"
    report_is_created = HTTPStatus.OK
    metrics_map = METRICS_MAP
    REPORTING_PERIOD = 95

    def report_init_endpoint(self, record_type: str) -> str:
        return f"/{self.API_VERSION}/reports"

    def _download_report(self, report_info: ReportInfo, url: str) -> List[dict]:
        """
        Download and parse report result
        """
        return super()._download_report(None, url)

    def _get_init_report_body(self, report_date: str, record_type: str, profile):
        metrics_list = self.metrics_map[record_type]

        reportTypeId = "spCampaigns"
        group_by = ["campaign", "campaignPlacement"]
        filters = []

        if record_type == "adGroups":
            group_by.append("adGroup")

        elif record_type == "productAds":
            reportTypeId = "spAdvertisedProduct"
            group_by = ["advertiser"]

        elif "asin" in record_type:
            reportTypeId = "spPurchasedProduct"
            group_by = ["asin"]

        elif record_type == "keywords" or record_type == "targets":
            group_by = ["targeting"]
            reportTypeId = "spTargeting"
            filters = [{"field": "keywordType", "values": ["TARGETING_EXPRESSION", "TARGETING_EXPRESSION_PREDEFINED"]}]

            if record_type == "keywords":
                filters = [
                    {
                        "field": "keywordType",
                        "values": ["BROAD", "PHRASE", "EXACT", "TARGETING_EXPRESSION", "TARGETING_EXPRESSION_PREDEFINED"]
                    },
                    {
                        "field": "adKeywordStatus",
                        "values": [
                            "ENABLED",
                            "PAUSED",
                            "ARCHIVED"
                        ]
                    }
                ]

        elif record_type == "searchTerm":
            group_by = ["searchTerm"]
            reportTypeId = "spSearchTerm"
            filters = [
                {
                    "field": "keywordType",
                    "values": ["BROAD", "PHRASE", "EXACT", "TARGETING_EXPRESSION", "TARGETING_EXPRESSION_PREDEFINED"]
                }
            ]

        body = {
            "name": f"{record_type} report {report_date}",
            "startDate": report_date,
            "endDate": report_date,
            "configuration": {
                "adProduct": self.ad_product,
                "groupBy": group_by,
                "columns": metrics_list,
                "reportTypeId": reportTypeId,
                "filters": filters,
                "timeUnit": "SUMMARY",
                "format": "GZIP_JSON"
            }
        }

        return body
