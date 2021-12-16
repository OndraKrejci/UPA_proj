
# Projekt UPA

* Autoři: Ondřej Krejčí (xkrejc69), Oliver Kuník (xkunik00)
* Téma projektu: 03: COVID-19

## 1. část
Stažení a uložení dat do databáze.

### Požadavky
* Python3.7 s moduly z `part1/requirements.txt`
* MongoDB

Skript očekává, že databáze běží a je dostupná při jeho spuštění, ve výchozím nastavení ji očekává na localhost:27017. Je možné použít například databázi na virtuálním stroji používaném na cvičeních z UPA (https://rychly-edu.gitlab.io/dbs/nosql/nixos-dbs-vm/) s nastaveným port forwardingem pro host port a guest port 27017.

### Spuštění
`python3 part1_main.py [--host host] [--port port] [--timeout timeout]`

## 2. část
Načtení dat pro zvolené dotazy z databáze. Vytvoření grafů a tabulek dle zadání dotazů. Příprava dat pro dolovací úlohu.

### Požadavky
* Python3.7 s moduly z `requirements.txt`
* MongoDB s daty uloženými v první části (jen pro vytváření CSV souborů)

### Vytvoření CSV
Řešeno ve skriptu `csv_create.py`, který z databáze načte požadovaná data a uloží je do výstupních csv souborů ve složce `data_csv/` (vytvořené soubory už jsou součástí odevzdání). Skript vyžaduje, aby běžela databáze s očekávanými daty z první části projektu.

`python3 csv_create.py [--host host] [--port port] [--timeout timeout]`

### Vytvoření grafů a tabulek
Skript `plot_graphs.py` načítá data z vytvořených CSV souborů, provádí případné úpravy a vytváří z nich výstupní vizualizace. Skript při spuštění grafy zobrazí a zároveň je uloží do složky `output/`. Tabulky z dotazu B1 jsou ve formátu `txt` také exportovány do této složky. Grafické výstupy z tohoto skriptu jsou prezentovány v dokumentaci. Také je možné si jednotlivé grafy zobrazit v notebooku `plot_graphs.ipynb`.

### Příprava dat pro dolovací úlohu
Přípravu dat zajišťuje skript `prepare_dm.py`. Skript načítá data ze vstupního souboru `data_csv/C1-orp_ctvrtleti.csv` a upravená data uloží do souboru `output/C1-orp_ctvrtleti-upraveno.csv`.
