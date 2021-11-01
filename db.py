
import pymongo

import json
import csv
from dateutil import parser as DateParser

from download import DATA_PATH

from collections import OrderedDict

from merge import mergeListsByKey

class DBC:
    DB_NAME = 'covid'

    def __init__(self) -> None:
        self.connect()
    
    def connect(self, host: str = 'localhost', port: int = 27017) -> None:
        self.conn = pymongo.MongoClient(host, port)
        self.db = self.conn[DBC.DB_NAME]

    def delete_db(self) -> None:
        self.conn.drop_database(DBC.DB_NAME)

    def get_collection(self, name: str, drop: bool = True):
        coll = self.db[name]

        if drop:
            coll.drop()

        return coll

    def create_collection_hosptializace_cr(self) -> None:
        coll = self.get_collection('hospitalizace_cr')

        document = []

        with open('%s/%s' % (DATA_PATH, 'cr-hospitalizace-umrti.json'), 'r') as file:
            json_data = json.load(file)

        for data in json_data['data']:
            document.append(self.create_record_hospitalizace_cr(data))

        coll.insert_many(document)

    def create_record_hospitalizace_cr(self, data: dict) -> dict:
        return {
            'datum': DateParser.parse(data['datum']),
            'pacient_prvni_zaznam': data.get('pacient_prvni_zaznam', 0),
            'kum_pacient_prvni_zaznam': data.get('kum_pacient_prvni_zaznam', 0),
            'pocet_hosp': data.get('pocet_hosp', 0),
            'stav_bez_priznaku': data.get('stav_bez_priznaku', 0),
            'stav_lehky': data.get('stav_lehky', 0),
            'stav_stredni': data.get('stav_stredni', 0),
            'stav_tezky': data.get('stav_tezky', 0),
            'jip': data.get('jip', 0),
            'kyslik': data.get('kyslik', 0),
            'hfno': data.get('hfno', 0),
            'upv': data.get('upv', 0),
            'ecmo': data.get('ecmo', 0),
            'tezky_upv_ecmo': data.get('tezky_upv_ecmo', 0),
            'umrti': data.get('umrti', 0),
            'kum_umrti': data.get('kum_umrti', 0)
        }

    def create_collection_obyvatelstvo(self) -> None:
        coll = self.get_collection('obyvatelstvo')

        document = []

        with open('%s/%s' % (DATA_PATH, 'kraj-okres-obyvatelstvo.csv'), 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            for data in reader:
                document.append(self.create_record_obyvatelstvo(data))

        coll.insert_many(document)

    def create_record_obyvatelstvo(self, data: OrderedDict) -> dict:
        return {
            'pocet': data['hodnota'],
            'pohlavi_cis': data['pohlavi_kod'],
            'vek_cis': data['vek_cis'],
            'vek_kod': data['vek_kod'], # CSU7700
            'vuzemi_cis': data['vuzemi_cis'],
            'vuzemi_kod': data['vuzemi_kod'],
            'casref_do': DateParser.parse(data['casref_do']),
            'pohlavi_txt': data['pohlavi_txt'],
            'vek_txt': data['vek_txt'],
            'vuzemi_txt': data['vuzemi_txt']
        }
        
    def create_collection_covid_po_dnech_cr(self) -> None:
        coll = self.get_collection('covid_po_dnech_cr')

        document = []

        with open('%s/%s' % (DATA_PATH, 'cr-hospitalizace-umrti.json'), 'r') as file:
            hospitalizace = json.load(file)['data']
        
        with open('%s/%s' % (DATA_PATH, 'cr-nakazeni-vyleceni-umrti-testy.json'), 'r') as file:
            nakazeni = json.load(file)['data']
        
        with open('%s/%s' % (DATA_PATH, 'cr-testy.json'), 'r') as file:
            testy = json.load(file)['data']
        
        l1 = [{**i1, **i2} for i1, i2 in mergeListsByKey(hospitalizace, nakazeni, key="datum")]
        l2 = [{**i1, **i2} for i1, i2 in mergeListsByKey(l1, testy, key="datum")]
        

        for data in l2:
            document.append(self.create_record_covid_po_dnech_cr(data))

        coll.insert_many(document)
        print(document)
    
    def create_record_covid_po_dnech_cr(self, data: OrderedDict) -> dict:
        return {
            'datum': DateParser.parse(data['datum']),
            'AG_pozit_asymp_PCR_conf': data.get('AG_pozit_asymp_PCR_conf', 0),
            'AG_pozit_symp': data.get('AG_pozit_symp', 0),
            'PCR_pozit_asymp': data.get('PCR_pozit_asymp', 0),
            'PCR_pozit_sympt': data.get('PCR_pozit_sympt', 0),
            'ecmo': data.get('ecmo', 0),
            'hfno': data.get('hfno', 0),
            'jip': data.get('jip', 0),
            'kum_pacient_prvni_zaznam': data.get('kum_pacient_prvni_zaznam', 0),
            'kum_umrti': data.get('kum_umrti', 0),
            'kumulativni_pocet_ag_testu': data.get('kumulativni_pocet_ag_testu', 0),
            'kumulativni_pocet_nakazenych': data.get('kumulativni_pocet_nakazenych', 0),
            'kumulativni_pocet_testu': data.get('kumulativni_pocet_testu', 0),
            'kumulativni_pocet_umrti': data.get('kumulativni_pocet_umrti', 0),
            'kumulativni_pocet_vylecenych': data.get('kumulativni_pocet_vylecenych', 0),
            'kyslik': data.get('kyslik', 0),
            'pacient_prvni_zaznam': data.get('pacient_prvni_zaznam', 0),
            'pocet_hosp': data.get('pocet_hosp', 0),
            'pozit_typologie_test_indik_diagnosticka': data.get('pozit_typologie_test_indik_diagnosticka', 0),
            'pozit_typologie_test_indik_epidemiologicka': data.get('pozit_typologie_test_indik_epidemiologicka', 0),
            'pozit_typologie_test_indik_ostatni': data.get('pozit_typologie_test_indik_ostatni', 0),
            'pozit_typologie_test_indik_preventivni': data.get('pozit_typologie_test_indik_preventivni', 0),
            'prirustkovy_pocet_nakazenych': data.get('prirustkovy_pocet_nakazenych', 0),
            'prirustkovy_pocet_provedenych_ag_testu': data.get('prirustkovy_pocet_provedenych_ag_testu', 0),
            'prirustkovy_pocet_provedenych_testu': data.get('prirustkovy_pocet_provedenych_testu', 0),
            'prirustkovy_pocet_umrti': data.get('prirustkovy_pocet_umrti', 0),
            'prirustkovy_pocet_vylecenych': data.get('prirustkovy_pocet_vylecenych', 0),
            'stav_bez_priznaku': data.get('stav_bez_priznaku', 0),
            'stav_lehky': data.get('stav_lehky', 0),
            'stav_stredni': data.get('stav_stredni', 0),
            'stav_tezky': data.get('stav_tezky', 0),
            'tezky_upv_ecmo': data.get('tezky_upv_ecmo', 0),
            'typologie_test_indik_diagnosticka': data.get('typologie_test_indik_diagnosticka', 0),
            'typologie_test_indik_epidemiologicka': data.get('typologie_test_indik_epidemiologicka', 0),
            'typologie_test_indik_ostatni': data.get('typologie_test_indik_ostatni', 0),
            'typologie_test_indik_preventivni': data.get('typologie_test_indik_preventivni', 0),
            'umrti': data.get('umrti', 0),
            'upv': data.get('upv', 0),
        }

if __name__ == '__main__':
    dbc = DBC()
    dbc.delete_db()
    dbc.create_collection_hosptializace_cr()
    dbc.create_collection_obyvatelstvo()
    dbc.create_collection_covid_po_dnech_cr()
