##
# @file main.py
# @author Ondřej Krejčí xkrejc69@stud.fit.vutbr.cz
# @author Oliver Kuník xkunik00@stud.fit.vutbr.cz
# Subject: UPA - Data Storage and Preparation
# @date: 11/2021
# Parse downloaded data and imports them to DB

import pymongo

import sys
import json
import csv
from dateutil import parser as DateParser

from typing import Union

from download import DATA_PATH
from ciselniky import UZEMI_KRAJ, Kraje, ORP, get_csu7700_ciselnik

from collections import OrderedDict

from merge import mergeListsByKey, mergeListsByTwoKeys

from xls import get_obyvatelia_orp

class DBC:
    DB_NAME = 'covid'
    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 27017

    def __init__(self, host: str = 'localhost', port: int = 27017, timeout: Union[int, None] = None) -> None:
        self.conn = None
        self.db = None
        self.connect(host, port, timeout)

    def __del__(self):
        if self.conn:
            self.conn.close()

    def connect(self, host: str, port: int, timeout: Union[int, None] = None) -> None:
        timeout = timeout if timeout is not None else 15000

        self.conn = pymongo.MongoClient(host, port, serverSelectionTimeoutMS=timeout)

        try:
            self.conn.admin.command('ping')
        except pymongo.errors.ConnectionFailure:
            print('Failed to connect to the server at %s:%i' % (host, port), file=sys.stderr)
            sys.exit(1)

        self.db = self.conn[DBC.DB_NAME]

    def delete_db(self) -> None:
        self.conn.drop_database(DBC.DB_NAME)

    def get_collection(self, name: str, drop: bool = False):
        coll = self.db[name]

        if drop:
            coll.drop()

        return coll

    def create_collection_obyvatelstvo_kraj(self) -> None:
        coll = self.get_collection('obyvatelstvo_kraj')

        kraje = Kraje()

        min_datum = DateParser.parse('2018-01-01')
        document = []

        with open('%s/%s' % (DATA_PATH, 'kraj-okres-obyvatelstvo.csv'), 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            for data in reader:
                casref_do = DateParser.parse(data['casref_do'])
                vuzemi_cis = int(data['vuzemi_cis']) if data['vuzemi_cis'] else None
                if vuzemi_cis == UZEMI_KRAJ and casref_do > min_datum:
                    try:
                        kod = int(data['vuzemi_kod'])
                    except:
                        kod = ''
                    nuts = kraje.get_nuts(kod)
                    document.append(self.create_record_obyvatelstvo_kraj(data, casref_do, kod, nuts))

        coll.insert_many(document)

    def create_record_obyvatelstvo_kraj(self, data: OrderedDict, casref_do, kod, nuts) -> dict:
        return {
            'pocet': int(data['hodnota']) if data['hodnota'] else None,
            'pohlavi_kod': int(data['pohlavi_kod']) if data['pohlavi_kod'] else '', # 1=muz, 2=zena
            'vek_kod': data['vek_kod'], # CSU7700
            'vek_txt': data['vek_txt'],
            'kod': kod,
            'nuts_kod': nuts,
            'nazev': data['vuzemi_txt'],
            'casref_do': casref_do
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

    def create_record_covid_po_dnech_cr(self, data: dict) -> dict:
        return {
            'datum': DateParser.parse(data['datum']),
            'AG_pozit_asymp_PCR_conf': data.get('AG_pozit_asymp_PCR_conf', None),
            'AG_pozit_symp': data.get('AG_pozit_symp', None),
            'PCR_pozit_asymp': data.get('PCR_pozit_asymp', None),
            'PCR_pozit_sympt': data.get('PCR_pozit_sympt', None),
            'ecmo': data.get('ecmo', None),
            'hfno': data.get('hfno', None),
            'jip': data.get('jip', None),
            'kum_pacient_prvni_zaznam': data.get('kum_pacient_prvni_zaznam', None),
            'kum_umrti': data.get('kum_umrti', None),
            'kumulativni_pocet_ag_testu': data.get('kumulativni_pocet_ag_testu', None),
            'kumulativni_pocet_nakazenych': data.get('kumulativni_pocet_nakazenych', None),
            'kumulativni_pocet_testu': data.get('kumulativni_pocet_testu', None),
            'kumulativni_pocet_umrti': data.get('kumulativni_pocet_umrti', None),
            'kumulativni_pocet_vylecenych': data.get('kumulativni_pocet_vylecenych', None),
            'kyslik': data.get('kyslik', None),
            'pacient_prvni_zaznam': data.get('pacient_prvni_zaznam', None),
            'pocet_hosp': data.get('pocet_hosp', None),
            'pozit_typologie_test_indik_diagnosticka': data.get('pozit_typologie_test_indik_diagnosticka', None),
            'pozit_typologie_test_indik_epidemiologicka': data.get('pozit_typologie_test_indik_epidemiologicka', None),
            'pozit_typologie_test_indik_ostatni': data.get('pozit_typologie_test_indik_ostatni', None),
            'pozit_typologie_test_indik_preventivni': data.get('pozit_typologie_test_indik_preventivni', None),
            'prirustkovy_pocet_nakazenych': data.get('prirustkovy_pocet_nakazenych', None),
            'prirustkovy_pocet_provedenych_ag_testu': data.get('prirustkovy_pocet_provedenych_ag_testu', None),
            'prirustkovy_pocet_provedenych_testu': data.get('prirustkovy_pocet_provedenych_testu', None),
            'prirustkovy_pocet_umrti': data.get('prirustkovy_pocet_umrti', None),
            'prirustkovy_pocet_vylecenych': data.get('prirustkovy_pocet_vylecenych', None),
            'stav_bez_priznaku': data.get('stav_bez_priznaku', None),
            'stav_lehky': data.get('stav_lehky', None),
            'stav_stredni': data.get('stav_stredni', None),
            'stav_tezky': data.get('stav_tezky', None),
            'tezky_upv_ecmo': data.get('tezky_upv_ecmo', None),
            'typologie_test_indik_diagnosticka': data.get('typologie_test_indik_diagnosticka', None),
            'typologie_test_indik_epidemiologicka': data.get('typologie_test_indik_epidemiologicka', None),
            'typologie_test_indik_ostatni': data.get('typologie_test_indik_ostatni', None),
            'typologie_test_indik_preventivni': data.get('typologie_test_indik_preventivni', None),
            'umrti': data.get('umrti', None),
            'upv': data.get('upv', None)
        }

    def create_collection_nakazeni_vek_okres_kraj(self) -> None:
        coll = self.get_collection('nakazeni_vek_okres_kraj')

        document = []

        with open('%s/%s' % (DATA_PATH, 'kraj-okres-nakazeni.json'), 'r') as file:
            json_data = json.load(file)['data']

        for data in json_data:
            document.append(self.create_record_nakazeni_vek_okres_kraj(data))

        coll.insert_many(document)

    def create_record_nakazeni_vek_okres_kraj(self, data: dict) -> dict:
        return {
            'datum': DateParser.parse(data['datum']),
            'vek': data.get('vek', None),
            'pohlavi': data.get('pohlavi', None),
            'kraj_nuts_kod': data.get('kraj_nuts_kod', None),
            'okres_lau_kod': data.get('okres_lau_kod', None),
            'nakaza_v_zahranici': data.get('nakaza_v_zahranici', None),
            'nakaza_zeme_csu_kod': data.get('nakaza_zeme_csu_kod', None)
        }

    def create_collection_umrti_vek_okres_kraj(self) -> None:
        coll = self.get_collection('umrti_vek_okres_kraj')

        document = []

        with open('%s/%s' % (DATA_PATH, 'kraj-okres-umrti.json'), 'r') as file:
            json_data = json.load(file)['data']

        for data in json_data:
            document.append(self.create_record_umrti_vek_okres_kraj(data))

        coll.insert_many(document)

    def create_record_umrti_vek_okres_kraj(self, data: dict) -> dict:
        return {
            'datum': DateParser.parse(data['datum']),
            'vek': data.get('vek', None),
            'pohlavi': data.get('pohlavi', None),
            'kraj_nuts_kod': data.get('kraj_nuts_kod', None),
            'okres_lau_kod': data.get('okres_lau_kod', None)
        }

    def create_collection_vyleceni_vek_okres_kraj(self) -> None:
        coll = self.get_collection('vyleceni_vek_okres_kraj')

        document = []

        with open('%s/%s' % (DATA_PATH, 'kraj-okres-vyleceni.json'), 'r') as file:
            json_data = json.load(file)['data']

        for data in json_data:
            document.append(self.create_record_vyleceni_vek_okres_kraj(data))

        coll.insert_many(document)

    def create_record_vyleceni_vek_okres_kraj(self, data: dict) -> dict:
        return {
            'datum': DateParser.parse(data['datum']),
            'vek': data.get('vek', None),
            'pohlavi': data.get('pohlavi', None),
            'kraj_nuts_kod': data.get('kraj_nuts_kod', None),
            'okres_lau_kod': data.get('okres_lau_kod', None)
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
            if data['datum'] != datum or data['kraj_nuts_kod'] != kraj:
                testy_merged.append(data)
                datum = data['datum']
                kraj = data['kraj_nuts_kod']

        with open('%s/%s' % (DATA_PATH, 'kraj-okres-nakazeni-vyleceni-umrti.json'), 'r') as file:
            nakazeni_vyleceni_umrti = json.load(file)['data']

        nakazeni_vyleceni_umrti = [i for i in nakazeni_vyleceni_umrti if i['kraj_nuts_kod']]

        nakazeni_vyleceni_umrti = sorted(nakazeni_vyleceni_umrti, key=lambda x: (x['datum'], x['kraj_nuts_kod']))

        datum = nakazeni_vyleceni_umrti[0]['datum']
        kraj = nakazeni_vyleceni_umrti[0]['kraj_nuts_kod']
        nakazeni = 0
        vyleceni = 0
        umrti = 0
        nakazeni_vyleceni_umrti_merged = []
        for data in nakazeni_vyleceni_umrti:
            if data['datum'] != datum or data['kraj_nuts_kod'] != kraj:
                nakazeni_vyleceni_umrti_merged.append({
                    "datum": datum,
                    "kraj_nuts_kod": kraj,
                    "kumulativni_pocet_nakazenych": nakazeni,
                    "kumulativni_pocet_vylecenych": vyleceni,
                    "kumulativni_pocet_umrti": umrti
                })
                datum = data['datum']
                kraj = data['kraj_nuts_kod']
                nakazeni = data['kumulativni_pocet_nakazenych']
                vyleceni = data['kumulativni_pocet_vylecenych']
                umrti = data['kumulativni_pocet_umrti']
            else:
                nakazeni += data['kumulativni_pocet_nakazenych']
                vyleceni += data['kumulativni_pocet_vylecenych']
                umrti += data['kumulativni_pocet_umrti']

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

    def create_record_nakazeni_vyleceni_umrti_testy_kraj(self, data: dict) -> dict:
        return {
            'datum': DateParser.parse(data['datum']),
            'kraj_nuts_kod': data.get('kraj_nuts_kod', None),
            'kumulativni_pocet_nakazenych': data.get('kumulativni_pocet_nakazenych', None),
            'kumulativni_pocet_umrti': data.get('kumulativni_pocet_umrti', None),
            'kumulativni_pocet_vylecenych': data.get('kumulativni_pocet_vylecenych', None),
            'kumulativni_pocet_prvnich_testu': data.get('kumulativni_pocet_prvnich_testu_kraj', None),
            'kumulativni_pocet_testu': data.get('kumulativni_pocet_testu_kraj', None),
            'prirustkovy_pocet_prvnich_testu': data.get('prirustkovy_pocet_prvnich_testu_kraj', None),
            'prirustkovy_pocet_testu': data.get('prirustkovy_pocet_testu_kraj', None)
        }

    def create_collection_ockovani_orp(self) -> None:
        coll = self.get_collection('ockovani_orp')

        document = []

        with open('%s/%s' % (DATA_PATH, 'orp-ockovani-geografie.json'), 'r', encoding='utf-8') as file:
            ockovani = json.load(file)['data']

        ockovani = [i for i in ockovani if i['orp_bydliste_kod']]

        ockovani = sorted(ockovani, key=lambda x: (x['datum'], x['orp_bydliste_kod']))

        datum = ockovani[0]['datum']
        orp = ockovani[0]['orp_bydliste_kod']
        orp_nazev = ockovani[0]['orp_bydliste']
        kraj = ockovani[0]['kraj_nazev']
        nuts = ockovani[0]['kraj_nuts_kod']
        pocet = 0
        ockovani_merged = []
        for data in ockovani:
            if data['datum'] != datum or data['orp_bydliste_kod'] != orp:
                ockovani_merged.append({
                    "datum": datum,
                    "kraj_nuts_kod": nuts,
                    "kraj_nazev": kraj,
                    "orp_kod": orp,
                    "pocet_davek": pocet,
                    "orp_nazev": orp_nazev
                })
                datum = data['datum']
                orp = data['orp_bydliste_kod']
                kraj = data['kraj_nazev']
                nuts = data['kraj_nuts_kod']
                orp_nazev = data['orp_bydliste']
                pocet = data['pocet_davek']
            else:
                pocet += data['pocet_davek']

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
            'orp_kod': data.get('orp_kod', None),
            'orp_nazev': data.get('orp_nazev', None),
            'kraj_nuts_kod': data.get('kraj_nuts_kod', None),
            'kraj_nazev': data.get('kraj_nazev', None),
            'pocet_davek': data.get('pocet_davek', None)
        }

    def create_collection_nakazeni_orp(self) -> None:
        coll = self.get_collection('nakazeni_orp')

        document = []

        with open('%s/%s' % (DATA_PATH, 'obce-nakazeni.json'), 'r', encoding='utf-8') as file:
            nakazeni = json.load(file)['data']

        nakazeni = [i for i in nakazeni if i['orp_kod']]

        nakazeni = sorted(nakazeni, key=lambda x: (x['datum'], x['orp_kod']))

        den = nakazeni[0]['den']
        datum = nakazeni[0]['datum']
        nuts = nakazeni[0]['kraj_nuts_kod']
        kraj = nakazeni[0]['kraj_nazev']
        lau = nakazeni[0]['okres_lau_kod']
        okres = nakazeni[0]['okres_nazev']
        orp = nakazeni[0]['orp_kod']
        orp_nazev = nakazeni[0]['orp_nazev']
        nove = 0
        aktivni = 0
        nove65 = 0
        nove7 = 0
        nove14 = 0

        nakazeni_merged = []
        for data in nakazeni:
            if data['datum'] != datum or data['orp_kod'] != orp:
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
                den = data['den']
                datum = data['datum']
                nuts = data['kraj_nuts_kod']
                kraj = data['kraj_nazev']
                lau = data['okres_lau_kod']
                okres = data['okres_nazev']
                orp = data['orp_kod']
                orp_nazev = data['orp_nazev']
                nove = data['nove_pripady']
                aktivni = data['aktivni_pripady']
                nove65 = data['nove_pripady_65']
                nove7 = data['nove_pripady_7_dni']
                nove14 = data['nove_pripady_14_dni']
            else:
                nove += data['nove_pripady']
                aktivni += data['aktivni_pripady']
                nove65 += data['nove_pripady_65']
                nove7 += data['nove_pripady_7_dni']
                nove14 += data['nove_pripady_14_dni']

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
                document.append(self.create_record_nakazeni_orp(data))

        coll.insert_many(document)

    def create_record_nakazeni_orp(self, data: dict) -> dict:
        return {
            'datum': DateParser.parse(data['datum']),
            'kraj_nuts_kod': data.get('kraj_nuts_kod', None),
            'kraj_nazev': data.get('kraj_nazev', None),
            'okres_lau_kod': data.get('okres_lau_kod', None),
            'okres_nazev': data.get('okres_nazev', None),
            'orp_kod': data.get('orp_kod', None),
            'orp_nazev': data.get('orp_nazev', None),
            'nove_pripady': data.get('nove_pripady', None),
            'aktivni_pripady': data.get('aktivni_pripady', None),
            'nove_pripady_65': data.get('nove_pripady_65', None),
            'nove_pripady_7_dni': data.get('nove_pripady_7_dni', None),
            'nove_pripady_14_dni': data.get('nove_pripady_14_dni', None)
        }

    def create_collection_nakazeni_hospitalizovani_orp(self) -> None:
        coll = self.get_collection('nakazeni-hospitalizovani-orp')

        document = []

        with open('%s/%s' % (DATA_PATH, 'orp-nakazeni-hospitalizovani.json'), 'r', encoding='utf-8') as file:
            json_data = json.load(file)['data']

        for data in json_data:
            document.append(self.create_record_nakazeni_hospitalizovani_orp(data))

        coll.insert_many(document)

    def create_record_nakazeni_hospitalizovani_orp(self, data: OrderedDict) -> dict:
        return {
            'den': data.get('den', None),
            'datum': DateParser.parse(data['datum']),
            'orp_kod': data.get('orp_kod', None),
            'orp_nazev': data.get('orp_nazev', None),
            'incidence_7': data.get('incidence_7', None),
            'incidence_65_7': data.get('incidence_65_7', None),
            'incidence_75_7': data.get('incidence_75_7', None),
            'prevalence': data.get('prevalence', None),
            'prevalence_65': data.get('prevalence_65', None),
            'prevalence_75': data.get('prevalence_75', None),
            'aktualni_pocet_hospitalizovanych_osob': data.get('aktualni_pocet_hospitalizovanych_osob', None),
            'nove_hosp_7': data.get('nove_hosp_7', None),
            'testy_7': data.get('testy_7', None)
        }

    def create_collection_obyvatele_orp(self) -> None:
        coll = self.get_collection('obyvatele_orp')
        coll.insert_many(get_obyvatelia_orp(DATA_PATH))

    def create_collection_umrti_cr(self) -> None:
        coll = self.get_collection('umrti_cr')

        min_datum = DateParser.parse('2018-01-01')
        document = []
        with open('%s/%s' % (DATA_PATH, 'cr-zemreli.csv'), 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            for data in reader:
                casref_od = DateParser.parse(data['casref_od'])
                if casref_od > min_datum:
                  document.append(self.create_record_umrti_cr(data, casref_od))

        coll.insert_many(document)

    def create_record_umrti_cr(self, data: OrderedDict, casref_od) -> dict:
        return {
            'pocet': int(data['hodnota']) if data['hodnota'] else None,
            'vek_kod': data['vek_kod'], # CSU 7700
            'vek_txt': data['vek_txt'],
            'casref_od': casref_od,
            'casref_do': DateParser.parse(data['casref_do']),
            'priznak': data['priznak']
        }

    def create_collection_obyvatele_orp2(self) -> None:
        coll = self.get_collection('obyvatele_orp2')

        orp = ORP()
        kraje = Kraje()

        document = []

        with open('%s/%s' % (DATA_PATH, 'orp-populace.csv'), 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for data in reader:
                try:
                    orp_kod = int(data['vuzemi_kod'])
                except:
                    orp_kod = ''

                nuts_kod = None
                if orp_kod:
                    kraj_kod = orp.get_kraj_kod(orp_kod)
                    if kraj_kod:
                        nuts_kod = kraje.get_nuts(kraj_kod)

                document.append(self.create_record_obyvatele_orp2(data, orp_kod, nuts_kod))

        cis = get_csu7700_ciselnik()
        document = sorted(document, key=lambda x: (x['casref_do'], x['orp_kod'], x['pohlavi_kod']))
        l = []
        i = {'casref_do': document[0]['casref_do'],
            'orp_kod' : document[0]['orp_kod'],
            'kraj_nuts_kod' : document[0]['kraj_nuts_kod'],
            'orp_nazev' : document[0]['orp_nazev'],
            'pohlavi_kod' : document[0]['pohlavi_kod']
            }
        m = {}
        for data in document:
            if data['casref_do'] == i['casref_do'] and data['orp_kod'] == i['orp_kod'] and data['pohlavi_kod'] == i['pohlavi_kod']:
                m[data['vek_kod']] = data['pocet']
            else:
                summ = 0
                for j in range(0, 15):
                    if cis[j] in m:
                        summ += m[cis[j]]
                i['0-14'] = summ
                summ = 0
                for j in range(15, 60):
                    if cis[j] in m:
                        summ += m[cis[j]]
                i['15-59'] = summ
                summ = 0
                for j in range(60, 100):
                    if cis[j] in m:
                        summ += m[cis[j]]
                i['60+'] = summ
                l.append(i)
                i = {}
                m = {}
                i = {'casref_do': data['casref_do'],
                    'orp_kod' : data['orp_kod'],
                    'kraj_nuts_kod' : data['kraj_nuts_kod'],
                    'orp_nazev' : data['orp_nazev'],
                    'pohlavi_kod' : data['pohlavi_kod']
                    }
        summ = 0
        for j in range(0, 15):
            if cis[j] in m:
                summ += m[cis[j]]
        i['0-14'] = summ
        summ = 0
        for j in range(15, 60):
            if cis[j] in m:
                summ += m[cis[j]]
        i['15-59'] = summ
        summ = 0
        for j in range(60, 100):
            if cis[j] in m:
                summ += m[cis[j]]
        i['60+'] = summ
        l.append(i)

        coll.insert_many(l)

    def create_record_obyvatele_orp2(self, data: OrderedDict, orp_kod, nuts_kod) -> dict:
        return {
            'pocet': int(data['hodnota']) if data['hodnota'] else None,
            'pohlavi_kod': int(data['pohlavi_kod']) if data['pohlavi_kod'] else '', # 1=muz, 2=zena
            'vek_kod': data['vek_kod'], # CSU7700
            'vek_txt': data['vek_txt'],
            'orp_kod': orp_kod,
            'kraj_nuts_kod': nuts_kod,
            'orp_nazev': data['vuzemi_txt'],
            'casref_do': DateParser.parse(data['casref_do'])
        }

    def create_all_collections(self) -> None:
        self.delete_db()

        self.create_collection_obyvatelstvo_kraj()
        self.create_collection_covid_po_dnech_cr()
        self.create_collection_nakazeni_vek_okres_kraj()
        self.create_collection_nakazeni_vyleceni_umrti_testy_kraj()
        self.create_collection_ockovani_orp()
        self.create_collection_nakazeni_orp()
        self.create_collection_umrti_vek_okres_kraj()
        self.create_collection_vyleceni_vek_okres_kraj()
        self.create_collection_nakazeni_hospitalizovani_orp()
        self.create_collection_obyvatele_orp()
        self.create_collection_umrti_cr()
        self.create_collection_obyvatele_orp2()

if __name__ == '__main__':
    dbc = DBC()
    dbc.create_all_collections()
