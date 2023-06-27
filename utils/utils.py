import os
import json
from random import randrange
from datetime import timedelta
import re

class Utils:
    
    VIEWNAMES = {
        'Bas':'Basilicata',
        'Tos':'Toscana',
        'Cam':'Campania',
        'Abr':'Abruzzo',
        'Lom':'Lombardia',
        'Pie':'Piemonte',
        'Laz':'Lazio',
        'Cal':'Calabria',
        'Sic':'Sicilia',
        'Sar':'Sardegna',
        'Pug':'Puglia',
        'Mol':'Molise',
        'Mar':'Marche',
        'Lig':'Liguria',
        'Umb':'Umbria',
        'Ven':'Veneto'
    }
        
    def __init__(self, nomefile):
        with open(nomefile,'r') as j:
           self.italia = json.load(j) 
    
    @staticmethod
    def return_view_name_from_code(code):
        return Utils.VIEWNAMES[code]
    
    @staticmethod
    def prendi_mese(numero_mese):
        mesi = ['Gennaio', 'Febbraio', 'Marzo', 'Aprile', 'Maggio', 'Giugno',
                'Luglio', 'Agosto', 'Settembre', 'Ottobre', 'Novembre', 'Dicembre']
        if numero_mese < 1 or numero_mese > 12:
            raise ValueError('Il numero di mese deve essere compreso tra 1 e 12')
        return mesi[numero_mese - 1]
    
    def get_regioni(self):
        return [elem['nome'] for elem in self.italia['regioni']]
            
    def get_province(self, reg):
        for regioni in self.italia['regioni']:
            if regioni['nome'] == reg:
                return regioni['province']
    
    def get_province_nome(self, reg):
        n_prov = []
        for regioni in self.italia['regioni']:
            if regioni['nome'] == reg:
                n_prov.extend(province['nome'] for province in regioni['province'])
        return n_prov
    
    def get_comuni(self, reg, prov):
        for regioni in self.italia['regioni']:
            if regioni['nome'] == reg:
                for province in regioni['province']:
                    if province['nome'] == prov:
                        return province['comuni']

    def get_comuni_nome(self, reg, prov):
        n_com = []
        for regioni in self.italia['regioni']:
            if regioni['nome'] == reg:
                for province in regioni['province']:
                    if province['nome'] == prov:
                        n_com.extend(comuni['nome'] for comuni in province['comuni'])
        return n_com
    
    def get_cap(self, reg, prov, com):
        for regioni in self.italia['regioni']:
            if regioni['nome'].lower() == reg.lower():
                for province in regioni['province']:
                    if province['nome'].lower() == prov.lower():
                        for comuni in province['comuni']:
                            if re.search(comuni['nome'].lower(),com.lower(),re.IGNORECASE):
                                return comuni['cap']
    
    def get_reg_prov_com_from_cap(self, cap):
        regionslst, provincelst, comunilst = [],[],[]
        for regioni in self.italia['regioni']:
            for province in regioni['province']:
                for comuni in province['comuni']:
                    if comuni['cap'] == cap:
                        regionslst.append(regioni['nome'])
                        provincelst.append(province['nome'])
                        comunilst.append(comuni['nome'])
        return regionslst,provincelst,comunilst
    
    def random_date(self, start, end):
        """
        This function will return a random datetime between two datetime 
        objects.
        """
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = randrange(int_delta)
        return start + timedelta(seconds=random_second)
    
    @staticmethod
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]