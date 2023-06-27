from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
from salesforce_bulk.salesforce_bulk import BulkBatchFailed, BulkApiError
from salesforce_bulk.util import IteratorBytesIO
from salesforce_api import Salesforce
import pandas as pd
from tqdm import tqdm
import os
import unicodecsv
import json
from utils.logger import create_logger
from datetime import datetime
from utils.convert_dtypes import convert
from utils.utils import Utils

def query_all_records(sf,table_name,logger):
    job = sf.create_query_job(str(table_name), contentType='CSV')
    batch = sf.query(job, "select Id,Zip_Code__c from "+str(table_name)+" WHERE Zip_Code__c != 'nan'")
    sf.wait_for_batch(job, batch)
    sf.close_job(job)

    all_ids = []
    for result in sf.get_all_results_for_query_batch(batch):
        reader = unicodecsv.DictReader(result, encoding='utf-8')
        for row in reader:
            single = {}
            single['Id'] = row['Id']
            single['Zip_Code__c'] = row['Zip_Code__c'].replace('.0','')
            all_ids.append(single)
    logger.warning("*"*50+"Retrieved all {} zip codes.".format(table_name)+"*"*50)
    return all_ids

def update_all_zip_codes(sf,all_data,table_name,logger):
    n = 9999
    x = list(Utils.divide_chunks(all_data, n))
    for el in tqdm(x,total=len(x),desc='Importing...'):
        csv_iter = CsvDictsAdapter(iter(el))
        job = sf.create_update_job(table_name,contentType='CSV',concurrency='Parallel')
        batch = sf.post_batch(job, csv_iter)
        sf.wait_for_batch(job, batch)
        for result in sf.get_batch_results(batch_id=batch, job_id=job):
            if result.success == 'false':
                sf.close_job(job)
                logger.error("ERROR : "+str(result.error))
                raise BulkBatchFailed(job,batch,state_message=result.error)
        sf.close_job(job)


def refactor_zipcode(table_name, is_test=False, is_sdo=False):
    logger = create_logger('logs/logfile_new.log')
    environment = 'Test' if is_test else 'SDO' if is_sdo else 'Production'
    logger.warning("*"*50+"Initializing Salesforce {} environment...".format(environment)+"*"*50)
    if is_test:
        sf = SalesforceBulk(username=os.getenv('SANDBOX_USERNAME'),password=os.getenv('SANDBOX_PASSWORD'), security_token=os.getenv('SANDBOX_SECURITY_TOKEN'),host=os.getenv('SANDBOX_HOST'), domain='test')
        sfapi = Salesforce(username=os.getenv('SANDBOX_USERNAME'),password=os.getenv('SANDBOX_PASSWORD'), security_token=os.getenv('SANDBOX_SECURITY_TOKEN'), domain=os.getenv('SANDBOX_HOST'), is_sandbox=True)
    elif is_sdo:
        sf = SalesforceBulk(username=os.getenv('SDO_USERNAME'),password=os.getenv('SDO_PASSWORD'), security_token=os.getenv('SDO_SECURITY_TOKEN'))
        sfapi = Salesforce(username=os.getenv('SDO_USERNAME'),password=os.getenv('SDO_PASSWORD'), security_token=os.getenv('SDO_SECURITY_TOKEN'))
    else:
        sf = SalesforceBulk(username=os.getenv('PROD_USERNAME'),password=os.getenv('PROD_PASSWORD'), security_token=os.getenv('PROD_SECURITY_TOKEN'))
        sfapi = Salesforce(username=os.getenv('PROD_USERNAME'),password=os.getenv('PROD_PASSWORD'), security_token=os.getenv('PROD_SECURITY_TOKEN'))

    logger.warning("*"*50+"Updating {} zip codes...".format(table_name)+"*"*50)
    all_datas = query_all_records(sf, table_name, logger)
    update_all_zip_codes(sf,all_datas,table_name,logger)




