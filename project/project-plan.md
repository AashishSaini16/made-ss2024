# Project Plan
## Title
Analyzing the daily storage and flow of Gas in France.

## Main Question
What are the overall trends in gas storage and flow over the past years?

## Description
Analyzing overall trends in gas storage levels over the past year provides critical insights into energy security, market stability, operational efficiency, and strategic planning. It helps stakeholders anticipate and respond to changes in the energy landscape, ensuring a reliable and affordable gas supply.

## Datasources
### Datasource1: Daily stock in gas storage
* Metadata URL: https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/stock-quotidien-stockages-gaz/exports/csv
* Data URL: https://www.data.gouv.fr/fr/datasets/r/6f5cc4ea-ef1c-4a1d-b7c4-ad49f3970933
* Data Type: CSV

This dataset presents the gas stock present in the Teréga and Storengy gas storages, by PITS and at the end of the gas day since November 1, 2010 (GWh PCS 0°C).

### Datasource2: Daily flow rate from gas storage
* Metadata URL: https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/debit-quotidien-stockages-gaz/exports/csv
* Data URL: https://www.data.gouv.fr/fr/datasets/r/670bf487-69dc-4b38-9255-c71a0dc1c41f
* Data Type: CSV

This dataset presents the accumulation of gas flow rates (injection/withdrawal) between the gas transport network and the gas storages of Teréga and Storengy, by PITS and during a gas day since November 1, 2010 (GWh/d PCS 0°C).

## Issues
### Issue1: Missing Data
The dataset contains records of random dates of the month and the data was recorded in French so it requires the completion of data for more correlation and translation as well.

### Issue2: Data Quality
The data quality of the dataset needs to be checked as the data is not frequently updated and specified.

### Issue3: Data Merging
The data from the two datasets needs to be merged to analyze the storage and flow of resources.
