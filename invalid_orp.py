
import json

from download import DATA_PATH
from ciselniky import ORP

"""
Výpočet nesprávných kódů ORP
"""

orp_helper = ORP()

def invalid_orp_ockovani():
    global orp_helper
    
    with open('%s/%s' % (DATA_PATH, 'orp-ockovani-geografie.json'), 'r', encoding='utf-8') as file:
        datalist = json.load(file)['data']

    missing_orp = set()
    for data in datalist:
        if data['orp_bydliste_kod'] not in orp_helper.orp.keys():
            missing_orp.add((data['orp_bydliste'], data['orp_bydliste_kod']))

    #print(missing_orp)
    return missing_orp

def invalid_orp_incidence():
    global orp_helper

    with open('%s/%s' % (DATA_PATH, 'orp-nakazeni-hospitalizovani.json'), 'r', encoding='utf-8') as file:
        datalist = json.load(file)['data']

    missing_orp = set()
    for data in datalist:
        if data['orp_kod'] not in orp_helper.orp.keys():
            missing_orp.add((data['orp_nazev'], data['orp_kod']))

    #print(missing_orp)
    return missing_orp

def invalid_orp_obec():
    global orp_helper

    with open('%s/%s' % (DATA_PATH, 'obce-nakazeni.json'), 'r', encoding='utf-8') as file:
        datalist = json.load(file)['data']

        missing_orp = set()
        for data in datalist:
            if data['orp_kod'] not in orp_helper.orp.keys():
                missing_orp.add((data['orp_nazev'], data['orp_kod']))

    #print(missing_orp)
    return missing_orp

# predpocitane hodnoty
OCKOVANI = {('Hořovice', 2008), ('Pohořelice', 6413), ('Humpolec', 6303), (None, None), ('Židlochovice', 6421), ('Bohumín', 8002), ('Světlá nad Sázavou', 6311), ('Vítkov', 8022), ('Sedlčany', 2023), ('Frenštát pod Radhoštěm', 8005), ('Blansko', 6401), ('Říčany', 2022), ('Český Těšín', 8004), ('Bystřice nad Pernštejnem', 6301), ('Jablunkov', 8010), ('Pacov', 6309), ('Veselí nad Moravou', 6418), ('Moravské Budějovice', 6306), ('Příbram', 2020), ('Třinec', 8021), ('Třebíč', 6313), ('Jihlava', 6305), ('Kladno', 2009), ('Mikulov', 6411), ('Kolín', 2010), ('Nové Město na Moravě', 6308), ('Nymburk', 2018), ('Mnichovo Hradiště', 2016), ('Brno', 6403), ('Slavkov u Brna', 6415), ('Poděbrady', 2019), ('Bruntál', 8003), ('Vlašim', 2025), ('Havlíčkův Brod', 6302), ('Frýdlant nad Ostravicí', 8007), ('Votice', 2026), ('Neratovice', 2017), ('Hustopeče', 6407), ('Beroun', 2002), ('Čáslav', 2004), ('Dobříš', 2007), ('Havířov', 8008), ('Hlučín', 8009), ('Nový Jičín', 8015), ('Vyškov', 6419), ('Český Brod', 2006), ('Hodonín', 6406), ('Brandýs nad Labem-Stará Boleslav', 2003), ('Karviná', 8011), ('Mělník', 2014), ('Kopřivnice', 8012), ('Opava', 8017), ('Telč', 6312), ('Slaný', 2024), ('Kuřim', 6409), ('Rakovník', 2021), ('Mladá Boleslav', 2015), ('Lysá nad Labem', 2013), ('Boskovice', 6402), ('Kralupy nad Vltavou', 2011), ('Náměšť nad Oslavou', 6307), ('Kutná Hora', 2012), ('Odry', 8016), ('Benešov', 2001), ('Bučovice', 6405), ('Šlapanice', 6416), ('Kyjov', 6410), ('Znojmo', 6420), ('Rýmařov', 8020), ('Břeclav', 6404), ('Velké Meziříčí', 6314), ('Chotěboř', 6304), ('Pelhřimov', 6310), ('Bílovec', 8001), ('Černošice', 2005), ('Žďár nad Sázavou', 6315), ('Rosice', 6414), ('Kravaře', 8013), ('Orlová', 8018), ('Moravský Krumlov', 6412), ('Krnov', 8014), ('Frýdek-Místek', 8006), ('Tišnov', 6417), ('Ostrava', 8019), ('Ivančice', 6408)}
INCIDENCE = {('Hořovice', 2008), ('Pohořelice', 6413), ('Humpolec', 6303), ('Židlochovice', 6421), ('Bohumín', 8002), ('Světlá nad Sázavou', 6311), ('Vítkov', 8022), ('Sedlčany', 2023), ('Frenštát pod Radhoštěm', 8005), ('Blansko', 6401), ('Říčany', 2022), ('Český Těšín', 8004), ('Bystřice nad Pernštejnem', 6301), ('Jablunkov', 8010), ('Pacov', 6309), ('Moravské Budějovice', 6306), ('Veselí nad Moravou', 6418), ('Příbram', 2020), ('Třinec', 8021), ('Třebíč', 6313), ('Jihlava', 6305), ('Kladno', 2009), ('Mikulov', 6411), ('Kolín', 2010), ('Nové Město na Moravě', 6308), ('Nymburk', 2018), ('Mnichovo Hradiště', 2016), ('Brno', 6403), ('Slavkov u Brna', 6415), ('Poděbrady', 2019), ('Bruntál', 8003), ('Vlašim', 2025), ('Havlíčkův Brod', 6302), ('Frýdlant nad Ostravicí', 8007), ('Votice', 2026), ('Neratovice', 2017), ('Hustopeče', 6407), ('Beroun', 2002), ('Čáslav', 2004), ('Dobříš', 2007), ('Havířov', 8008), ('Hlučín', 8009), ('Nový Jičín', 8015), ('Vyškov', 6419), ('Český Brod', 2006), ('Brandýs n.L.- St.Boleslav', 2003), ('Hodonín', 6406), ('Karviná', 8011), ('Mělník', 2014), ('Kopřivnice', 8012), ('Opava', 8017), ('Telč', 6312), ('Praha', 0), ('Slaný', 2024), ('Kuřim', 6409), ('Rakovník', 2021), ('Mladá Boleslav', 2015), ('Lysá nad Labem', 2013), ('Boskovice', 6402), ('Kralupy nad Vltavou', 2011), ('Náměšť nad Oslavou', 6307), ('Kutná Hora', 2012), ('Odry', 8016), ('Benešov', 2001), ('Bučovice', 6405), ('Šlapanice', 6416), ('Kyjov', 6410), ('Znojmo', 6420), ('Rýmařov', 8020), ('Břeclav', 6404), ('Velké Meziříčí', 6314), ('Chotěboř', 6304), ('Pelhřimov', 6310), ('Bílovec', 8001), ('Černošice', 2005), ('Žďár nad Sázavou', 6315), ('Rosice', 6414), ('Kravaře', 8013), ('Orlová', 8018), ('Moravský Krumlov', 6412), ('Krnov', 8014), ('Frýdek-Místek', 8006), ('Tišnov', 6417), ('Ostrava', 8019), ('Ivančice', 6408)}
OBEC_ORP = {('Hořovice', 2008), ('Pohořelice', 6413), ('Humpolec', 6303), (None, None), ('Židlochovice', 6421), ('Bohumín', 8002), ('Světlá nad Sázavou', 6311), ('Vítkov', 8022), ('Sedlčany', 2023), ('Frenštát pod Radhoštěm', 8005), ('Blansko', 6401), ('Říčany', 2022), ('Český Těšín', 8004), ('Bystřice nad Pernštejnem', 6301), ('Jablunkov', 8010), ('Pacov', 6309), ('Moravské Budějovice', 6306), ('Veselí nad Moravou', 6418), ('Třinec', 8021), ('Příbram', 2020), ('Třebíč', 6313), ('Jihlava', 6305), ('Kladno', 2009), ('Mikulov', 6411), ('Kolín', 2010), ('Nové Město na Moravě', 6308), ('Nymburk', 2018), ('Mnichovo Hradiště', 2016), ('Brno', 6403), ('Slavkov u Brna', 6415), ('Poděbrady', 2019), ('Bruntál', 8003), ('Vlašim', 2025), ('Frýdlant nad Ostravicí', 8007), ('Havlíčkův Brod', 6302), ('Votice', 2026), ('Neratovice', 2017), ('Hustopeče', 6407), ('Beroun', 2002), ('Čáslav', 2004), ('Dobříš', 2007), ('Havířov', 8008), ('Hlučín', 8009), ('Nový Jičín', 8015), ('Vyškov', 6419), ('Český Brod', 2006), ('Hodonín', 6406), ('Brandýs nad Labem-Stará Boleslav', 2003), ('Karviná', 8011), ('Mělník', 2014), ('Kopřivnice', 8012), ('Opava', 8017), ('Telč', 6312), ('Slaný', 2024), ('cizinci', 9999), ('Kuřim', 6409), ('Rakovník', 2021), ('Mladá Boleslav', 2015), ('Lysá nad Labem', 2013), ('Boskovice', 6402), ('Kralupy nad Vltavou', 2011), ('Náměšť nad Oslavou', 6307), ('Kutná Hora', 2012), ('Odry', 8016), ('Benešov', 2001), ('Bučovice', 6405), ('Šlapanice', 6416), ('Kyjov', 6410), ('Znojmo', 6420), ('Rýmařov', 8020), ('Břeclav', 6404), ('Velké Meziříčí', 6314), ('Chotěboř', 6304), ('Pelhřimov', 6310), ('Bílovec', 8001), ('Černošice', 2005), ('Žďár nad Sázavou', 6315), ('Rosice', 6414), ('Kravaře', 8013), ('Orlová', 8018), ('Moravský Krumlov', 6412), ('Krnov', 8014), ('Frýdek-Místek', 8006), ('Tišnov', 6417), ('Ostrava', 8019), ('Ivančice', 6408)}

if __name__ == '__main__':
    ockovani = invalid_orp_ockovani()
    incidence = invalid_orp_incidence()
    obec = invalid_orp_obec()
