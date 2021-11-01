
import pymongo

import json
import csv
from dateutil import parser as DateParser

from download import DATA_PATH

from collections import OrderedDict

from merge import mergeListsByKey, mergeListsByTwoKeys

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
        
    def create_collection_nakazeni_vek_okres_kraj(self) -> None:
        coll = self.get_collection('nakazeni_vek_okres_kraj')

        document = []

        with open('%s/%s' % (DATA_PATH, 'kraj-okres-nakazeni.json'), 'r') as file:
            json_data = json.load(file)['data']
        

        for data in json_data:
            document.append(self.create_record_nakazeni_vek_okres_kraj(data))

        coll.insert_many(document)
    
    def create_record_nakazeni_vek_okres_kraj(self, data: OrderedDict) -> dict:
        return {
            'datum': DateParser.parse(data['datum']),
            'vek': data.get('vek', 0),
            'pohlavy': data.get('pohlavy', 0),
            'kraj_nuts_kod': data.get('kraj_nuts_kod', 0),
            'okres_lau_kod': data.get('okres_lau_kod', 0),
            'nakaza_v_zahranici': data.get('nakaza_v_zahranici', 0),
            'nakaza_zeme_csu_kod': data.get('nakaza_zeme_csu_kod', 0),
        }
        
    def create_collection_nakazeni_vyleceni_umrti_testy_kraj(self) -> None:
        coll = self.get_collection('nakazeni_vyleceni_umrti_testy_kraj')
        
        document = []
        
        with open('%s/%s' % (DATA_PATH, 'kraj-okres-testy.json'), 'r') as file:
            testy = json.load(file)['data']
        
        testy = sorted(testy, key=lambda x: (x['datum'], x['kraj_nuts_kod']))
        
        datum = ''
        kraj = ''
        testy_merged = []
        for data in testy:
            if data.get('datum') != datum or data.get('kraj_nuts_kod') != kraj:
                testy_merged.append(data)
                datum = data.get('datum')
                kraj = data.get('kraj_nuts_kod')
        
        with open('%s/%s' % (DATA_PATH, 'kraj-okres-nakazeni-vyleceni-umrti.json'), 'r') as file:
            nakazeni_vyleceni_umrti = json.load(file)['data']
            
        nakazeni_vyleceni_umrti = [i for i in nakazeni_vyleceni_umrti if i['kraj_nuts_kod']]
        
        nakazeni_vyleceni_umrti = sorted(nakazeni_vyleceni_umrti, key=lambda x: (x['datum'], x['kraj_nuts_kod']))
        
        datum = nakazeni_vyleceni_umrti[0].get('datum')
        kraj = nakazeni_vyleceni_umrti[0].get('kraj_nuts_kod')
        nakazeni = 0
        vyleceni = 0
        umrti = 0
        nakazeni_vyleceni_umrti_merged = []
        for data in nakazeni_vyleceni_umrti:
            if data.get('datum') != datum or data.get('kraj_nuts_kod') != kraj:
                nakazeni_vyleceni_umrti_merged.append({
                    "datum": datum,
                    "kraj_nuts_kod": kraj,
                    "kumulativni_pocet_nakazenych": nakazeni,
                    "kumulativni_pocet_vylecenych": vyleceni,
                    "kumulativni_pocet_umrti": umrti
                })
                datum = data.get('datum')
                kraj = data.get('kraj_nuts_kod')
                nakazeni = data.get('kumulativni_pocet_nakazenych')
                vyleceni = data.get('kumulativni_pocet_vylecenych')
                umrti = data.get('kumulativni_pocet_umrti')
            else:
                nakazeni += data.get('kumulativni_pocet_nakazenych')
                vyleceni += data.get('kumulativni_pocet_vylecenych')
                umrti += data.get('kumulativni_pocet_umrti')
        nakazeni_vyleceni_umrti_merged.append({
                    "datum": datum,
                    "kraj_nuts_kod": kraj,
                    "kumulativni_pocet_nakazenych": nakazeni,
                    "kumulativni_pocet_vylecenych": vyleceni,
                    "kumulativni_pocet_umrti": umrti
                })
        
        l = [{**i1, **i2} for i1, i2 in mergeListsByTwoKeys(testy_merged, nakazeni_vyleceni_umrti_merged, key1="datum", key2="kraj_nuts_kod")]
        
        for data in l:
            document.append(self.create_record_nakazeni_vyleceni_umrti_testy_kraj(data))

        coll.insert_many(document)
        
    def create_record_nakazeni_vyleceni_umrti_testy_kraj(self, data: OrderedDict) -> dict:
        return {
            'datum': DateParser.parse(data['datum']),
            'kraj_nuts_kod': data.get('kraj_nuts_kod', 0),
            'kumulativni_pocet_nakazenych': data.get('kumulativni_pocet_nakazenych', 0),
            'kumulativni_pocet_umrti': data.get('kumulativni_pocet_umrti', 0),
            'kumulativni_pocet_vylecenych': data.get('kumulativni_pocet_vylecenych', 0),
            'kumulativni_pocet_prvnich_testu': data.get('kumulativni_pocet_prvnich_testu_kraj', 0),
            'kumulativni_pocet_testu': data.get('kumulativni_pocet_testu_kraj', 0),
            'prirustkovy_pocet_prvnich_testu': data.get('prirustkovy_pocet_prvnich_testu_kraj', 0),
            'prirustkovy_pocet_testu': data.get('prirustkovy_pocet_testu_kraj', 0),
        }

    def create_collection_nakazeni_hospitalizovani_orp(self) -> None:
        coll = self.get_collection('nakazeni_hospitalizovani_orp')

        document = []

        with open('%s/%s' % (DATA_PATH, 'orp-nakazeni-hospitalizovani.json'), 'r', encoding='utf-8') as file:
            datalist = json.load(file)['data']

        datalist = sorted(datalist, key=lambda item: (item['datum'], item['orp_kod']))

        incidence = None
        orp_kod = None
        for data in datalist:
            if data['orp_nazev'] == 'Brandýs n.L.- St.Boleslav':
                data['orp_nazev'] = 'Brandýs nad Labem-Stará Boleslav'
            elif data['orp_nazev'] == 'Praha':
                data['orp_kod'] = 1000

            if orp_kod is not None and orp_kod != data['orp_kod']:
                incidence = None
                orp_kod = data['orp_kod']
            
            if incidence is not None:
                incidence = max(incidence - data['incidence_7'], 0)

            document.append(self.create_record_nakazeni_hospitalizovani_orp(data, incidence))

            if incidence is None:
                incidence = data['incidence_7']

        coll.insert_many(document)

    def create_record_nakazeni_hospitalizovani_orp(self, data: dict, incidence: int) -> dict:
        return {
            'datum': DateParser.parse(data['datum']),
            'orp_kod': data.get('orp_kod', 0),
            'orp_nazev': data.get('orp_nazev', ''),
            'incidence_7': data.get('incidence_7', 0),
            'incidence_65_7': data.get('incidence_65_7', 0),
            'incidence_75_7': data.get('incidence_75_7', 0),
            'incidence': incidence, # nove pripady nakazy
            'prevalence': data.get('prevalence', 0), # aktivni pripady nakazy
            'prevalence_65': data.get('prevalence_65', 0),
            'prevalence_75': data.get('prevalence_75', 0),
            'aktualni_pocet_hospitalizovanych_osob': data.get('aktualni_pocet_hospitalizovanych_osob', 0),
            'nove_hosp_7': data.get('nove_hosp_7', 0),
            'testy_7': data.get('testy_7', 0) # PCR
        }

if __name__ == '__main__':
    dbc = DBC()
    dbc.delete_db()
    dbc.create_collection_hosptializace_cr()
    dbc.create_collection_obyvatelstvo()
    dbc.create_collection_covid_po_dnech_cr()
    dbc.create_collection_nakazeni_vek_okres_kraj()
    dbc.create_collection_nakazeni_vyleceni_umrti_testy_kraj()
    dbc.create_collection_nakazeni_hospitalizovani_orp()
