# Mastercard Custom Dataloader by XCC

### Copyright (c) 2023 - Formisano Raffaele

Here in this repository, we have designed a simple dataloader that reads all the data from the 72 files provided by Mastercard related to Tourism Insight Dashboards and load them into Salesforce.

## Things to do

* Clone the Github repository
* Create a .env file with the following parameters
* Remember to put all the parameters from Sandbox, Production and SDO environment into .env file

```bash
LOGS_IMPORT=logs/logs_import.log
SANDBOX_HOST=<your-sandbox-url>
SANDBOX_USERNAME=<your-sandbox-user-name>
SANDBOX_PASSWORD=<your-sandbox-password>
SANDBOX_SECURITY_TOKEN=<your-sandbox-security-token>
PROD_USERNAME=<your-prod-user-name>
PROD_PASSWORD=<your-prod-password>
PROD_SECURITY_TOKEN=<your-prod-security-token>
SDO_USERNAME=<your-sdo-user-name>
SDO_PASSWORD=<your-sdo-password>
SDO_SECURITY_TOKEN=<your-sdo-security-token>
```

* Put the zip file into data folder
* Execute the script by following command

```bash
python3 main.py
```

* Create a CSV file containing all the 72 files with the option 1
* Import into Salesforce in Asset Object with option 2
* Refactor Zip Codes with option 3
* Refactor Municipalities with option 4
