
# Projekt UPA

* Autoři: Ondřej Krejčí (xkrejc69), Oliver Kuník (xkunik00)
* Téma projektu: 03: COVID-19

## 1. část
Stažení a uložení dat do databáze.

## Požadavky
* Python3.7 s moduly z `requirements.txt`
* MongoDB

Skript očekává, že databáze běží a je dostupná při jeho spuštění, ve výchozím nastavení ji očekává na localhost:27017. Je možné použít například databázi na virtuálním stroji používaném na cvičeních z UPA (https://rychly-edu.gitlab.io/dbs/nosql/nixos-dbs-vm/) s nastaveným port forwardingem pro host port a guest port 27017.

## Spuštění
`python3 main.py [--host host] [--port port] [--timeout timeout]`
