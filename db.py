
import pymongo

import json
import csv
from dateutil import parser as DateParser

from download import DATA_PATH
from ciselniky import UZEMI_KRAJ

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

    def create_collection_obyvatelstvo_kraj(self) -> None:
        coll = self.get_collection('obyvatelstvo_kraj')

        document = []

        with open('%s/%s' % (DATA_PATH, 'kraj-okres-obyvatelstvo.csv'), 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            for data in reader:
                vuzemi_cis = int(data['vuzemi_cis']) if data['vuzemi_cis'] else None
                if vuzemi_cis == UZEMI_KRAJ:
                    document.append(self.create_record_obyvatelstvo_kraj(data))

        coll.insert_many(document)

    def create_record_obyvatelstvo_kraj(self, data: OrderedDict) -> dict:
        return {
            'pocet': int(data['hodnota']) if data['hodnota'] else None,
            'pohlavi_kod': int(data['pohlavi_kod']) if data['pohlavi_kod'] else None, # 1=muz, 2=zena
            'vek_kod': data['vek_kod'], # CSU7700
            'vek_txt': data['vek_txt'],
            'kraj_kod': data['vuzemi_kod'],
            'kraj_nazev': data['vuzemi_txt'],
            'casref_do': DateParser.parse(data['casref_do'])
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
            'pohlavi': data.get('pohlavi', 0),
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

    def create_collection_ockovani_orp(self) -> None:
        coll = self.get_collection('ockovani_orp')

        document = []

        with open('%s/%s' % (DATA_PATH, 'orp-ockovani-geografie.json'), 'r', encoding='utf-8') as file:
            ockovani = json.load(file)['data']

        ockovani = [i for i in ockovani if i['orp_bydliste_kod']]

        ockovani = sorted(ockovani, key=lambda x: (x['datum'], x['orp_bydliste_kod']))

        datum = ockovani[0].get('datum')
        orp = ockovani[0].get('orp_bydliste_kod')
        orp_nazev = ockovani[0].get('orp_bydliste')
        kraj = ockovani[0].get('kraj_nazev')
        nuts = ockovani[0].get('kraj_nuts_kod')
        pocet = 0
        ockovani_merged = []
        for data in ockovani:
            if data.get('datum') != datum or data.get('orp_bydliste_kod') != orp:
                ockovani_merged.append({
                    "datum": datum,
                    "kraj_nuts_kod": nuts,
                    "kraj_nazev": kraj,
                    "orp_kod": orp,
                    "pocet_davek": pocet,
                    "orp_nazev": orp_nazev
                })
                datum = data.get('datum')
                orp = data.get('orp_bydliste_kod')
                kraj = data.get('kraj_nazev')
                nuts = data.get('kraj_nuts_kod')
                orp_nazev = data.get('orp_bydliste')
                pocet = data.get('pocet_davek')
            else:
                pocet += data.get('pocet_davek')
        ockovani_merged.append({
                    "datum": datum,
                    "kraj_nuts_kod": nuts,
                    "kraj_nazev": kraj,
                    "orp_kod": orp,
                    "pocet_davek": pocet,
                    "orp_nazev": orp_nazev
                })

        for data in ockovani_merged:
            document.append(self.create_record_ockovani_orp(data))

        coll.insert_many(document)

    def create_record_ockovani_orp(self, data: dict) -> dict:
        return {
            'datum': DateParser.parse(data['datum']),
            'orp_kod': data.get('orp_kod', 0),
            'orp_nazev': data.get('orp_nazev', ''),
            'kraj_nuts_kod': data.get('kraj_nuts_kod', 0),
            'kraj_nazev': data.get('kraj_nazev', 0),
            'pocet_davek': data.get('pocet_davek', 0)
        }

    def create_collection_nakazeni_obce(self) -> None:
        coll = self.get_collection('nakazeni_obce')

        document = []

        with open('%s/%s' % (DATA_PATH, 'obce-nakazeni.json'), 'r', encoding='utf-8') as file:
            nakazeni = json.load(file)['data']

        nakazeni = [i for i in nakazeni if i['orp_kod']]

        nakazeni = sorted(nakazeni, key=lambda x: (x['datum'], x['orp_kod']))

        den = nakazeni[0].get('den')
        datum = nakazeni[0].get('datum')
        nuts = nakazeni[0].get('kraj_nuts_kod')
        kraj = nakazeni[0].get('kraj_nazev')
        lau = nakazeni[0].get('okres_lau_kod')
        okres = nakazeni[0].get('okres_nazev')
        orp = nakazeni[0].get('orp_kod')
        orp_nazev = nakazeni[0].get('orp_nazev')
        nove = 0
        aktivni = 0
        nove65 = 0
        nove7 = 0
        nove14 = 0

        nakazeni_merged = []
        for data in nakazeni:
            if data.get('datum') != datum or data.get('orp_kod') != orp:
                nakazeni_merged.append({
                    "den": den,
                    "datum": datum,
                    "kraj_nuts_kod": nuts,
                    "kraj_nazev": kraj,
                    "okres_lau_kod": lau,
                    "okres_nazev": okres,
                    "orp_kod": orp,
                    "orp_nazev": orp_nazev,
                    "nove_pripady": nove,
                    "aktivni_pripady": aktivni,
                    "nove_pripady_65": nove65,
                    "nove_pripady_7_dni": nove7,
                    "nove_pripady_14_dni": nove14
                })
                den = data.get('den')
                datum = data.get('datum')
                nuts = data.get('kraj_nuts_kod')
                kraj = data.get('kraj_nazev')
                lau = data.get('okres_lau_kod')
                okres = data.get('okres_nazev')
                orp = data.get('orp_kod')
                orp_nazev = data.get('orp_nazev')
                nove = data.get('nove_pripady')
                aktivni = data.get('aktivni_pripady')
                nove65 = data.get('nove_pripady_65')
                nove7 = data.get('nove_pripady_7_dni')
                nove14 = data.get('nove_pripady_14_dni')
            else:
                nove += data.get('nove_pripady')
                aktivni += data.get('aktivni_pripady')
                nove65 += data.get('nove_pripady_65')
                nove7 += data.get('nove_pripady_7_dni')
                nove14 += data.get('nove_pripady_14_dni')

        nakazeni_merged.append({
                    "den": den,
                    "datum": datum,
                    "kraj_nuts_kod": nuts,
                    "kraj_nazev": kraj,
                    "okres_lau_kod": lau,
                    "okres_nazev": okres,
                    "orp_kod": orp,
                    "orp_nazev": orp_nazev,
                    "nove_pripady": nove,
                    "aktivni_pripady": aktivni,
                    "nove_pripady_65": nove65,
                    "nove_pripady_7_dni": nove7,
                    "nove_pripady_14_dni": nove14
                })

        for data in nakazeni_merged:
            if type(data) is dict:
                document.append(self.create_record_nakazeni_obce(data))

        coll.insert_many(document)

    def create_record_nakazeni_obce(self, data: dict) -> dict:
        return {
            'datum': DateParser.parse(data['datum']),
            'kraj_nuts_kod': data.get('kraj_nuts_kod', None),
            'kraj_nazev': data.get('kraj_nazev', ''),
            'okres_lau_kod': data.get('okres_lau_kod', None),
            'okres_nazev': data.get('okres_nazev', ''),
            'orp_kod': data.get('orp_kod', None),
            'orp_nazev': data.get('orp_nazev', ''),
            'nove_pripady': data.get('nove_pripady', 0),
            'aktivni_pripady': data.get('aktivni_pripady', 0),
            'nove_pripady_65': data.get('nove_pripady_65', 0),
            'nove_pripady_7_dni': data.get('nove_pripady_7_dni', 0),
            'nove_pripady_14_dni': data.get('nove_pripady_14_dni', 0)
        }

    def create_all_collections() -> None:
        dbc.delete_db()
        dbc.create_collection_obyvatelstvo_kraj()
        dbc.create_collection_covid_po_dnech_cr()
        dbc.create_collection_nakazeni_vek_okres_kraj()
        dbc.create_collection_nakazeni_vyleceni_umrti_testy_kraj()
        dbc.create_collection_ockovani_orp()
        dbc.create_collection_nakazeni_obce()

if __name__ == '__main__':
    dbc = DBC()
    dbc.create_all_collections()
