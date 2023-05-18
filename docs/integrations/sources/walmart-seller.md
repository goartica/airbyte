# Walmart Seller

For syncing data for a walmart-seller connector, we need the **Client Id** & **Client Secret** from **My API Key**
1. Log in to [seller dashboard](https://seller.walmart.com) 
2. Browse to [API Keys](https://developer.walmart.com/account/generateKey) page
3. In **PRODUCTION KEYS** section, look for **My API Key** and note down the **Client Id** & **Client Secret**


**For Airbyte Open Source:**

1. Go to local Airbyte page.
2. In the left navigation bar, click **Sources**. In the top-right corner, click **+ new source**. 
3. Select **Walmart Seller** from the Source type dropdown. 
4. Enter the required `Client ID` and `Client Secret` for the seller account
5. Enter required `Start Date` and optional `End Date`
6. Click `Set up source`.

## Supported sync modes

The Walmart Seller source connector supports the following [sync modes](https://docs.airbyte.com/cloud/core-concepts/#connection-sync-mode):
 - Full Refresh

## Supported streams

### Raw Tables
- [Orders](https://developer.walmart.com/doc/us/us-mp/us-mp-orders/)
- [Returns](https://developer.walmart.com/doc/us/mp/us-mp-returns/)
- [Inventory](https://developer.walmart.com/doc/us/mp/us-mp-inventory/)
- [Items](https://developer.walmart.com/doc/us/mp/us-mp-items/)

### [On-Request Reports](https://developer.walmart.com/doc/us/us-mp/us-mp-onrequestreports/)
- [ItemReportsOnRequest](https://developer.walmart.com/doc/us/us-mp/us-mp-onrequestreports/)


**IMPORTANT: On-Request report streams must be configured in a separate connection and not mixed with any other stream category**

### [Pre-Generated Reports](https://developer.walmart.com/doc/us/us-mp/us-mp-reports/)
Coming soon
