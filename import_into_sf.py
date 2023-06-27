from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
from salesforce_bulk.salesforce_bulk import BulkBatchFailed, BulkApiError
from salesforce_bulk.util import IteratorBytesIO
from salesforce_api import Salesforce
import pandas as pd
import colorama
from colorama import Fore, Style
from tqdm import tqdm
import os
import unicodecsv
import json
from utils.logger import create_logger
from utils.utils import Utils
from datetime import datetime
from utils.convert_dtypes import convert
from dotenv import load_dotenv
load_dotenv(verbose=True)

def insert_function(df, sf, tablename, logger):
    n = 9999
    x = list(Utils.divide_chunks(df.to_dict('records'), n))
    for el in tqdm(x,total=len(x),desc='Importing...'):
        csv_iter = CsvDictsAdapter(iter(el))
        job = sf.create_insert_job(tablename,contentType='CSV',concurrency='Parallel')
        batch = sf.post_batch(job, csv_iter)
        sf.wait_for_batch(job, batch)
        for result in sf.get_batch_results(batch_id=batch, job_id=job):
            if result.success == 'false':
                sf.close_job(job)
                logger.error("ERROR : "+str(result.error))
                raise BulkBatchFailed(job,batch,state_message=result.error)
        sf.close_job(job)


def delete_function(sf, row, tablename):
    sf.bulk.delete(tablename,row)

def clear_objects(sfapi,table_name,logger):
    all_rec = sfapi.sobjects.query("SELECT Id FROM "+table_name+" LIMIT 49999")
    i = 1
    counter = len(all_rec)
    while counter > 0:
        all_ids = []
        for elem in all_rec:
            all_ids.append(elem['Id'])
        delete_function(sfapi, all_ids, table_name)
        logger.info("Deleted the # {} batch of {} ...".format(str(i),table_name))
        i += 1
        all_rec = sfapi.sobjects.query("SELECT Id FROM "+table_name+" LIMIT 49999")
        counter = len(all_rec)

def import_into_salesforce(table_name,filecsv,is_test=False,can_delete=False,is_sdo=False):
    logger = create_logger('logs/logfile_new.log')
    df = pd.read_csv(filecsv)
    df = convert(df)
    logger.warning("*"*50+"The dataframe have {} rows...".format(len(df))+"*"*50)
    df['Name'] = table_name
    environment = 'Test' if is_test else 'SDO' if is_sdo else 'Production'
    logger.warning("*"*50+"Initializing Salesforce {} environment...".format(environment)+"*"*50)
    if is_test:
        sf = SalesforceBulk(username=os.getenv('SANDBOX_USERNAME'),password=os.getenv('SANDBOX_PASSWORD'), security_token=os.getenv('SANDBOX_SECURITY_TOKEN'),host=os.getenv('SANDBOX_HOST'), domain='test')
        sfapi = Salesforce(username=os.getenv('SANDBOX_USERNAME'),password=os.getenv('SANDBOX_PASSWORD'), security_token=os.getenv('SANDBOX_SECURITY_TOKEN'), domain=os.getenv('SANDBOX_HOST'), is_sandbox=True)
        df['AccountId'] = '0015r00000jUtEZAA0'
        df['ContactId'] = '0035r00000Y14ZbAAJ'
    elif is_sdo:
        sf = SalesforceBulk(username=os.getenv('SDO_USERNAME'),password=os.getenv('SDO_PASSWORD'), security_token=os.getenv('SDO_SECURITY_TOKEN'))
        sfapi = Salesforce(username=os.getenv('SDO_USERNAME'),password=os.getenv('SDO_PASSWORD'), security_token=os.getenv('SDO_SECURITY_TOKEN'))
        df['AccountId'] = '0014S00000APXQ4QAP'
        df['ContactId'] = '0034S000008R06qQAC'
    else:
        sf = SalesforceBulk(username=os.getenv('PROD_USERNAME'),password=os.getenv('PROD_PASSWORD'), security_token=os.getenv('PROD_SECURITY_TOKEN'))
        sfapi = Salesforce(username=os.getenv('PROD_USERNAME'),password=os.getenv('PROD_PASSWORD'), security_token=os.getenv('PROD_SECURITY_TOKEN'))
        df['AccountId'] = '0016800000PIQMpAAP'
        df['ContactId'] = '0036800000IN0bzAAD'

    df = df.drop('Year_Month_Date__c',axis=1)
    # df2 = df[401959:]
    # df2['Week_Start_Date__c'] = df2['Week_Start_Date__c'].replace('nan','1999-12-31')
    df['Week_Start_Date__c'] = df['Week_Start_Date__c'].replace('nan','1999-12-31')
    # df2 = df[df['View__c'] == 'View_26_Basilicata__c']
    print(Fore.LIGHTYELLOW_EX + 'Are you sure you want to proceed with loading the data into Salesforce??' + Style.BRIGHT)
    can_proceed = input()
    can_proceed = can_proceed.lower()
    if can_proceed == 'yes' or can_proceed == 'y':
        print(Fore.LIGHTYELLOW_EX + 'There are already data into {} {}??'.format(environment,table_name) + Style.BRIGHT)
        can_proceed = input()
        can_proceed = can_proceed.lower()
        if can_proceed == 'yes' or can_proceed == 'y':
            if can_delete:
                logger.warning("*"*50+"Deleting records from {} {} object...".format(environment,table_name)+"*"*50)
                clear_objects(sfapi,table_name,logger)
            logger.warning("*"*50+"Ingesting in {} {}...".format(environment,table_name)+"*"*50)
            # insert_function(df2, sf, table_name,logger)
        insert_function(df, sf, table_name,logger)