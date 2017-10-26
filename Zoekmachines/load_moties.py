#load moties
import pandas as pd
from nltk.tokenize import RegexpTokenizer
from collections import Counter,defaultdict
from nltk.corpus import stopwords
import re
import numpy as np

kvrdf= pd.read_csv('http://maartenmarx.nl/teaching/zoekmachines/LectureNotes/MySQL/KVR.csv.gz',
                   compression='gzip', sep='\t', encoding='utf-8',
                   index_col=0, names=['jaar', 'partij','titel','vraag','antwoord','ministerie'])

kvrdf = kvrdf.dropna() #remove nan's
kvrdf.index=kvrdf.index.str.strip()
kvrdf.index=kvrdf.index.map(lambda x: str(x.split('.xml', 1)[0]))

kvrdf = kvrdf[kvrdf['antwoord'].map(len) > 1]
kvrdf = kvrdf[kvrdf['titel'].map(len) > 1]
kvrdf = kvrdf[kvrdf['jaar'].map(len) > 1]
kvrdf = kvrdf[kvrdf['partij'].map(len) > 1]
kvrdf = kvrdf[kvrdf['ministerie'].map(len) > 1]


from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

def ministerie_cleanup(x):
    x = re.sub(r'.*jus.*','Justitie',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*fin.*','Financien',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*erkeer.*','Verkeer en Waterstaat',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*algem.*','Algemene Zaken',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*bestu.*','Bestuurlijke Vernieuwing en Koninkrijksrelaties',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*binnen.*','Binnenlandse Zaken',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*buiten.*','Buitenlandse Zaken',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*def.*','Defensie',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*econo.*','Economische Zaken',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*europe.*','Europese Zaken',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*grote.*','Grote Steden- en Integratiebeleid',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*jeugd.*','Jeugd en Gezin',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*integra.*','Integratie, Jeugdbescherming, Preventie en Reclassering',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*kabine.*','Kabinet voor Nederlands-Antilliaanse en Arubaanse Zaken',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*voedsel.*','Landbouw, Natuur en Voedselkwaliteit',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*visser.*','Landbouw, Natuurbeheer en Visserij',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*minister.*','Minister-President',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*onderwij.*','Onderwijs, Cultuur en Wetenschap',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*ontwikkelings.*','Ontwikkelingssamenwerking',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*werkgelegen.*','Sociale Zaken en Werkgelegenheid',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*staten.*','Staten-Generaal',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*welzijn.*','Volksgezondheid, Welzijn en Sport',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*volkshuis.*','Volkshuisvesting, Ruimtelijke Ordening en Milieubeheer',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*vreemde.*','Vreemdelingenzaken en Integratie',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*wijken.*','Wonen, Wijken en Integratie',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*europe.*','Europese Zaken',x ,flags=re.IGNORECASE)

    return x

def partij_cleanup(x):
    x = re.sub(r'.*chris.*','CU',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*hendr.*','Groep Hendriks',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*bierman.*','Groep Bierman',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*groen.*','GroenLinks',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*eerdmans.*','Groep Eerdmans van Schijndel',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*lazrak.*','Groep Lazrak',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*nijpels.*','Groep Nijpels',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*wilders.*','Groep Wilders',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*justitie.*','NVT',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*batenbur.*','Martin Batenburg',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*arbeid.*','PvdA',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*dieren.*','PvdD',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*vrijheid.*','PVV',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*pvda.*','PvdA',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*RPF.*','RPF',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*SP.*','SP',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*Unie 55+.*','Ouderen Unie 55+',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*verdonk.*','Groep Verdonk',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*verkerk.*','Groep Verkerk',x ,flags=re.IGNORECASE)
    x = re.sub(r'.*cd.*','CDA',x ,flags=re.IGNORECASE)
    return x

k  = ({
        "_type":    "motie",
        "_index":   "moties",
        "_id":       x,
        "titel" :    kvrdf.titel[x],
        "partij" :   partij_cleanup(kvrdf.partij[x]),
        "jaar" :     kvrdf.jaar[x],
        "ministerie":ministerie_cleanup(kvrdf.ministerie[x]),
        "vraag":     kvrdf.vraag[x],
        "antwoord":  kvrdf.antwoord[x]
    } for x in kvrdf.index if x != 'KVR30348' )

es.indices.create('moties')
helpers.bulk(es,k )
