
import csv

from download import DATA_PATH

def load_vazby_orp_kraj():
    with open('%s/%s' % (DATA_PATH, 'vazba-orp-kraj-nuts.csv'), 'r', encoding='windows-1250') as file:
        header = [
            'KODJAZ',
            'TYPVAZ',
            'AKRCIS1',
            'KODCIS1',
            'orp_kod',
            'orp_nazev',
            'AKRCIS2',
            'KODCIS2',
            'kraj_kod',
            'kraj_nazev'
        ]
        reader = csv.DictReader(file, header)

        orp_data = []
        first = True
        for line in reader:
            if first:
                first = False
                continue

            orp_data.append({
                'orp_kod': line['orp_kod'],
                'orp_nazev': line['orp_nazev'],
                'kraj_kod': line['kraj_kod'],
                'kraj_nazev': line['kraj_nazev']
            })
            
    with open('vazba_orp_kraj.py', 'w', encoding='utf-8') as file:
        file.write('\nORP = {\n')
        for data in orp_data:
            file.write("\t'%s': {'orp_kod': %s, 'kraj_kod': %s, 'kraj_nazev': '%s'},\n" % (
                data['orp_nazev'],
                data['orp_kod'],
                data['kraj_kod'],
                data['kraj_nazev']
            ))
        file.write('}\n\n')

if __name__ == '__main__':
    load_vazby_orp_kraj()
