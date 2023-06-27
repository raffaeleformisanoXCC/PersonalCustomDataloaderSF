import pandas as pd
import os
from tqdm import tqdm
from utils.utils import Utils
from utils.logger import create_logger
from utils.fillnans import *
from datetime import datetime
from dotenv import load_dotenv
import zipfile
import pyzipper
load_dotenv(verbose=True)

def extract_zipfile(filename, path):
    with pyzipper.AESZipFile(r'{}'.format(filename), 'r', compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES) as zipref:
        zipref.extractall(r'data/{}'.format(path),pwd=str.encode('MC_MTI_Basilicatav3@?z'))

def populate_dataframe(path):
    df_globale = pd.DataFrame()
    for file in os.listdir(r'data/{}'.format(path)):
        if file.endswith('.csv'):
            if int(file.split('_')[3]) in [19,20,21,22,23,24,43,44,45,46,47,48,52,56,60,64,68,72]:
                #zip codes
                df = pd.read_csv(os.path.join('data/'+path+'/',file), dtype={'zip_code': 'category'})
            else:
                df = pd.read_csv(os.path.join('data/'+path+'/',file))
            df['View__c'] = str(file.split('_')[2]+'_'+file.split('_')[3]+'_'+Utils.return_view_name_from_code(file.split('_')[1])+'__c')
            dm = []
            for _,val in df.iterrows():
                dm.append(Utils.prendi_mese(val['Month'])+'-'+str(val['Year']))
            df['Year_Month_Date__c'] = dm
            df_globale = pd.concat([df_globale,df])
            del df
    return df_globale

def refactor_columns(df):
    diz = {}
    for elem in df.columns.tolist():
        old_elem = elem
        if str(elem).startswith('%'):
            el = elem.strip('.%_')
        elif str(elem) == 'zip_code':
            f = elem.split('_')[0]
            c = elem.split('_')[1]
            el = f.capitalize()+'_'+c.capitalize()
        elif str(elem) == 'Week_start_date':
            f = elem.split('_')[0]
            c = elem.split('_')[1]
            g = elem.split('_')[2]
            el = f.capitalize()+'_'+c.capitalize()+'_'+g.capitalize()
        elif str(elem) == 'View__c' or str(elem) == 'Year_Month_Date__c':
            diz[old_elem] = str(elem)
            continue
        else:
            el = elem
        diz[old_elem] = str(el)+'__c'
    return df.rename(columns=diz)


def create_dataset(zipfilename):
    #Take region name from zip file
    regionname = zipfilename.split('_')[3]
    OUTPATH = regionname+"DaCaricare"
    logger = create_logger(os.getenv('LOGS_IMPORT'))
    if not os.path.exists(r"data/{}".format(OUTPATH)):
        logger.warning("*"*50+"...Unzipping zip file in data folder..."+"*"*50)
        extract_zipfile(r"data/"+zipfilename,OUTPATH)
    logger.warning("*"*50+"...DATA CREATION..."+"*"*50)
    logger.info("*"*50+"Creating dataset from path..."+"*"*50)
    df = populate_dataframe(path=OUTPATH)
    logger.info("*"*50+"Rename columns of dataframe..."+"*"*50)
    df = refactor_columns(df)
    logger.info("*"*50+"Remove nans and empty values..."+"*"*50)
    df = fill(df)
    logger.info("*"*50+"Writing to file..."+"*"*50)
    df.to_csv('data/all_datanew_{}.csv'.format(datetime.now().strftime("%Y-%m-%d_%H%M%S")),index=False,encoding='utf-8')
    logger.warning("*"*50+"...DONE..."+"*"*50)