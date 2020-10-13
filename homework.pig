nazione = LOAD 'hdfs://localhost:9000/pigData/COVID-19-master/dati-andamento-nazionale/dpc-covid19-ita-andamento-nazionale.csv' USING PigStorage(',') as (data:datetime, stato:chararray, ricoverati_con_sintomi:int, terapia_intensiva:int, totale_ospedalizzati:int, isolamento_domiciliare:int, totale_positivi:int, variazione_totale_positivi:int, nuovi_positivi:int, dimessi_guariti:int, deceduti:int, totale_casi:int, tamponi:int, casi_testati:chararray, note_it:chararray, note_en:chararray);

regione = LOAD 'hdfs://localhost:9000/pigData/COVID-19-master/dati-regioni/dpc-covid19-ita-regioni.csv' USING PigStorage(',') as (data:datetime, stato:chararray, codice_regione:chararray, denominazione_regione:chararray, lat:chararray, long:chararray, ricoverati_con_sintomi:int, terapia_intensiva:int, totale_ospedalizzati:int, isolamento_domiciliare:int, totale_positivi:int, variazione_totale_positivi:int, nuovi_positivi:int, dimessi_guariti:int, deceduti:int, totale_casi:int, tamponi:int, casi_testati:int, note_it:chararray, note_en:chararray);


nazione1 = FOREACH nazione GENERATE data, totale_ospedalizzati, totale_positivi, dimessi_guariti, deceduti, tamponi;

regione1 = FOREACH regione GENERATE data, codice_regione,;

naz_reg = JOIN nazione1 by data, regione1 by data;

clean_naz_reg = FOREACH naz_reg GENERATE $0, $1, $2, $3, $4, $5, $7;

result = FILTER clean_naz_reg by (tamponi > 1000 AND deceduti > 10);

Dump result
