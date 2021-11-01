
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

        with open('vazba_orp_kraj.py', 'w', encoding='utf-8') as file:
            first = True
            file.write('\nORP = {\n')
            for line in reader:
                if first:
                    first = False
                    continue
                
                file.write("\t%s: {'nazev': '%s', 'kraj_kod': %s, 'kraj_nazev': '%s'},\n" % (
                    line['orp_kod'],
                    line['orp_nazev'],
                    line['kraj_kod'],
                    line['kraj_nazev']
                ))
            file.write('}\n')

if __name__ == '__main__':
    load_vazby_orp_kraj()
