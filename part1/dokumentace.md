
# Projekt UPA - 1. část

* Autoři: Ondřej Krejčí (xkrejc69), Oliver Kuník (xkunik00)
* Téma projektu: 03: COVID-19

## Zdroje dat a analýza
Veškerá data týkající se epidemie COVID-19 v ČR pocházejí z otevřených datových sad dostupných zde https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19.
Datové sady jsou dostupné ve formátech json a csv a ke každé sadě je dostupný stručný popis a schéma s popisem jednotlivých položek. Datové sady obsahují typické hodnoty související s epidemií, jako je počet nakažených, hospitalizovaných, informace o očkování ap. Také existují více specializované datové sady jako je např. přehled hospitalizací s ohledem na vykázaná očkování. Další data je možné hledat na https://data.gov.cz/datov%C3%A9-sady?dotaz=covid-19, ale u hlavních datových sad potřebných k řešení projektu se pouze odkazuje na https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19, je zde však možné nalézt další související data jako např. nákupy ochranných a zdravotnických pomůcek.

Pro data týkající se epidemie jsme použili první zdroj dat. Jsou zde dostupné datové sady pro různé hlavní atributy (jako jsou počty nakažených, testovaných, očkovaných atd.), většina datových sad obsahuje hodnoty pro jednotlivé dny. Datové sady se liší podle územního celku, pro který se hodnoty počítají. Největší územní celek je celá Česká republika, pro kterou jsou dostupné všechny hodnoty. Další územní jednotky jsou kraje, okresy, obce s rozšířenou působností a v jednom případě i obce. Zde už ale nejsou dostupné všechny sledované atributy pro každý územní celek. Hodnoty atributů mohou být kumulativní nebo přírůstkové, u atributů týkajících se dlouhodobějšího stavu jako je počet nakažených nebo hospitalizovaných navíc existují absolutní hodnoty pro dané časové období. Ne všechny z těchto hodnot atributů jsou dostupné.

Propojení mezi daty je možné přes územní celky použitím jejich identifikátorů. Kraje jsou identifikované pomocí jejich NUTS3 kódů, okresy pomocí LAU kódů a obce s rozšířenou působností (ORP) pomocí jejich číselníku. Přes tyto identifikátory je možné propojovat hodnoty pro menší územní celky a získat sumy pro větší. Datové sady typicky obsahují kódy nižšího i vyššího územního celku (např. okres i kraj). Propojení hodnot je tak možné provést pouze pomocí samotných hodnot atributu pro vyšší územní celek bez nutnosti použití dalších vazebních tabulek mezi jednotlivými identifikátory. Pro účely dotazů bude nutné získat požadované hodnoty identifikátorů pro požadované území nebo použít název. Hodnoty identifikátoru ORP u datových sad *COVID-19: Přehled epidemiologické situace dle hlášení krajských hygienických stanic podle ORP*, *COVID-19: Geografický přehled vykázaných očkování v čase*, *COVID-19: Epidemiologická charakteristika obcí* ne vždy odpovídají hodnotám z číselníku ÚZIS ČR (https://pzu-api.uzis.cz/api/orp), který je použitý pro popis daného atributu v popisu schématu. Hodnoty jsou ale unikátní a je tak možné je podle hodnoty identifikátoru propojit a případně sečíst. Pro účely dotazování ale bude nutné buďto část ORP s nesprávnými identifikátory nepoužívat nebo se na ně dotazovat pomocí názvu.

Data o počtech obyvatel jsou dostupná z datové sady ČSÚ *Obyvatelstvo podle pětiletých věkových skupin a pohlaví v krajích a okresech* (https://www.czso.cz/csu/czso/obyvatelstvo-podle-petiletych-vekovych-skupin-a-pohlavi-v-krajich-a-okresech). Datová sada je dostupná ve formátu csv a obsahuje počty obyvatel v každém roce rozdělené do skupin po 5 letech. Datová sada obsahuje data rozdělená podle pohlaví a územních celků (okresy, kraje, ČR). Typ hodnoty v řádku je určen hodnotami atributů identifikujícími věkovou skupinu, pohlaví a územní celek. Pro práci s daty bude nutné převzít identifikátory potřebných územních celků a věkových skupin. Také je dostupná datová sada o úmrtích za celou Českou republiku rozdělených do věkových skupin po týdnech.

Z ČSÚ je dále možné získat data o vazbě ORP na kraje (https://apl.czso.cz/iSMS/cisdata.jsp?kodcis=65), ale jak již bylo zmíněno, hodnoty identifikátorů ORP v datových sadách COVID-19 ne vždy odpovídají číselníku ORP. V datové sadě obyvatelstva není pro identifikaci krajů použit kód NUTS, proto je potřebný i číselník krajů ČSŮ. Dostupná je také populace jednotlivých ORP ve skupinách podle věku (https://www.czso.cz/csu/czso/obyvatelstvo-podle-jednotek-veku-a-pohlavi-ve-spravnich-obvodech-obci-s-rozsirenou-pusobnosti).

## Databáze a popis řešení
Pro uložení dat jsme zvolili databázi MongoDB, jelikož bude pro účely projektu nutné uložit velké množství různorodých dat. Nad daty se následně budou provádět dotazy pro získání jejich hodnot podle územních celků a času a případně se budou dále agregovat. Použití MongoDB je dále vhodné, jelikož většina primárních dat je ve formátu JSON, který může s drobnými úpravami být rovnou uložen do databáze. Další datové sady ve formátu csv je možné upravit a převést do JSON dokumentů a do databáze je možné uložit i libovolné další dokumenty, pokud by to bylo nutné.

Pro stažení a zpracování dat používáme skript v jazyce Python, který umožňuje snadné stažení dat ze zadaných url a následně jejich zpracování pomocí modulů `json` a `csv`. Pro práci s databází je použit modul `pymongo`. Skript stáhne všechna potřebná data a vytvoří jednotlivé kolekce z jednotlivých nebo sloučených datových sad, pro které provede další případné úpravy nebo výpočty hodnot.

Výběr datových sad a jejich případné spojování nebo slučování jejich hodnot provádíme primárně podle potřeb zvolených dotazů pro další část projektu. Ukládáme ale i další datové sady, pro potenciální využití u vlastních dotazů, ze stejného důvodu ponecháváme ve vytvářených kolekcích i hodnoty atributů, které nejsou nutně potřebné.

## Spuštění
Projekt pro spuštění vyžaduje Python3.7 s nainstalovanými moduly vypsanými v `requirements.txt`. Pro stažení a uložení dat do databáze slouží skript `main.py`. Skriptu je možné předat parametry pro připojení k databázi. Skript očekává, že databáze běží a je dostupná při jeho spuštění, ve výchozím nastavení ji očekává na localhost:27017. Je možné použít například databázi na virtuálním stroji používaném na cvičeních z UPA (https://rychly-edu.gitlab.io/dbs/nosql/nixos-dbs-vm/) s nastaveným port forwardingem pro host port a guest port 27017.

`python3 main.py [--host host] [--port port] [--timeout timeout]`

## Zvolené dotazy a potřebná data

Pozn. názvy kolekcí odpovídají názvu souborů s datovými sadami z https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19 pokud není uvedeno jinak.

### Dotaz A1
```
Vytvořte čárový (spojnicový) graf zobrazující vývoj covidové situace po měsících pomocí následujících hodnot: počet nově nakažených za měsíc, počet nově vyléčených za měsíc, počet nově hospitalizovaných osob za měsíc, počet provedených testů za měsíc. Pokud nebude výsledný graf dobře čitelný, zvažte logaritmické měřítko, nebo rozdělte hodnoty do více grafů.
```

Dotaz vyžaduje hodnoty přírůstků nakažených, vyléčených a hospitalizovaných a provedených testů za měsíc za celou Českou republiku. Celorepubliková data pro všechny potřebné hodnoty jsou poměrně snadno dostupná. Přehledová datová sada `nakazeni-vyleceni-umrti-testy.json` obsahuje přírůstky nakažených a vyléčených. Datová sada `hospitalizace.json` obsahuje přírůstky hospitalizovaných a `testy-pcr-antigenni.json` přírůstky provedených testů (rozděleno PCR a antigenní). Vzhledem k tomu, že data budou dotazována dohromady, tak budou uložená v jediné přehledové kolekci.

### Dotaz A2
```
Vytvořte krabicové grafy zobrazující rozložení věku nakažených osob v jednotlivých krajích.
```

Dotaz vyžaduje počty nakažených osob v jednotlivých krajích. Tato data jsou dostupná z datové sady `osoby.json`, která obsahuje záznamy o jednotlivých nakažených osobách včetně věku a NUTS kódu kraje.

### Dotaz B1
```
Sestavte 4 žebříčky krajů "best in covid" za poslední 4 čtvrtletí (1 čtvrtletí = 1 žebříček). Jako kritérium volte počet nově nakažených přepočtený na jednoho obyvatele kraje. Pro jedno čtvrtletí zobrazte výsledky také graficky. Graf bude pro každý kraj zobrazovat celkový počet nově nakažených, celkový počet obyvatel a počet nakažených na jednoho obyvatele. Graf můžete zhotovit kombinací dvou grafů do jednoho (jeden sloupcový graf zobrazí první dvě hodnoty a druhý, čárový graf, hodnotu třetí).
```

Zde jsou potřebné přírůstky nakažených pro jednotlivé kraje, pro účely přepočtu jsou také potřebné počty obyvatel krajů (jednotlivé hodnoty). Přírůstky nakažených pro jednotlivé kraje je možné získat ze sady `kraj-okres-nakazeni-vyleceni-umrti.json` obsahující kumulativní hodnoty nakažených podle okresů s informací o krajích. Jako počet obyvatel je možné použít nejnovější hodnoty pro daný kraj z datové sady ČSÚ *Obyvatelstvo podle pětiletých věkových skupin a pohlaví v krajích a okresech*.

### Dotaz C1
```
Hledání skupin podobných měst z hlediska vývoje covidu a věkového složení obyvatel.

- Atributy: počet nakažených za poslední 4 čtvrtletí, počet očkovaných za poslední 4 čtvrtletí, počet obyvatel ve věkové skupině 0..14 let, počet obyvatel ve věkové skupině 15 - 59, počet obyvatel nad 59 let.
- Pro potřeby projektu vyberte libovolně 50 měst, pro které najdete potřebné hodnoty (můžete např. využít nějaký žebříček 50 nejlidnatějších měst v ČR).
```

Pro tuto úlohu jsou potřebná data o přírůstcích nakažených a provedených očkováních podle obcí. Data o přírůstcích nakažených v jednotlivých obcích jsou dostupné v datové sadě `obce.json`. Data o očkování jsou ale nejjemněji dostupná na úrovni obcí s rozšířenou působností a to v sadě geografického přehledu očkování `ockovani-geografie.json`. Rozhodli jsme se proto úlohu řešit pro ORP, tj. hledat data na úrovni ORP a následně vybrat 50 ORP atd.

Na úrovni ORP ale nejsou dostupné údaje o přírůstcích nakažených, datová sada `orp.json` obsahuje pouze hodnoty za celé týdny a počet aktuálně nakažených. Potřebná data jsem tak získali sloučením hodnot pro obce.

Dále jsou potřebná data o populaci jednotlivých ORP rozdělených do věkových kategorií, která jsou dostupná z datové sady ČSÚ *Obyvatelstvo podle jednotek věku a pohlaví ve správních obvodech obcí s rozšířenou působností*.

## Vytvořené kolekce
* obyvatelstvo_kraj
    * počty obyvatel krajů za roční období rozdělené podle věkových skupin a pohlaví (včetně celkových sum)
* covid_po_dnech_cr
    * údaje o počtu nakažených, vyléčených, hospitalizovaných, úmrtích a očkováních za jednotlivé dny pro celou ČR
* nakazeni_vek_okres_kraj
    * údaje o jednotlivcích s prokázanou nákazou s informací o věku, kraji, okresu a datu
* nakazeni_vyleceni_umrti_testy_kraj
    * obsahuje kumulativní počty nakažených, úmrtí a vyléčených a kumulativní a přírůstkové počty provedených testů po dnech
* ockovani_orp
    * celkové počty provedených očkování po dnech podle ORP
* nakazeni_orp
    * počty nově nakažených a aktivních případů nákazy po dnech podle ORP
* umrti_vek_okres_kraj
    * údaje o jednotlivých úmrtích s informací o věku, kraji, okresu a datu
* vyleceni_vek_okres_kraj
    * údaje o jednotlivých vyléčených jedincích s informací o věku, kraji, okresu a datu
* nakazeni_hospitalizovani_orp
    * obsahuje aktuální počty hospitalizovaných, aktuálně nakažených a počtu nově nakažených a testovaných za týden pro ORP po dnech
    * také obsahuje údaje o nakažených ve skupinách 65+ a 75+
* obyvatele_orp
    * počty obyvatel jednotlivých ORP ve skupinách po 5 letech
* umrti_cr
    * údaje o úmrtích v ČR po týdnech podle věkových skupin
