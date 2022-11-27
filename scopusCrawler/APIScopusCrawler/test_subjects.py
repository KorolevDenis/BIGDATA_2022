import psycopg2
from xml.etree.ElementTree import ElementTree, fromstring, tostring
import requests

self.hostname = 'localhost'
self.username = 'denis'
self.password = 'qw'
self.database = 'denis'

self.connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database)
self.connection.set_client_encoding('UTF8')
self.cur = self.connection.cursor()

subject_dict = {
    'AGRI': 1,
    'ARTS': 2,
    'BIOC': 3,
    'BUSI': 4,
    'CENG': 5,
    'CHEM': 6,
    'COMP': 7,
    'DECI': 8,
    'DENT': 9,
    'EART': 10,
    'ECON': 11,
    'ENER': 12,
    'ENGI': 13,
    'ENVI': 14,
    'HEAL': 15,
    'IMMU': 16,
    'MATE': 17,
    'MATH': 18,
    'MEDI': 19,
    'NEUR': 20,
    'NURS': 21,
    'PHAR': 22,
    'PHYS': 23,
    'PSYC': 24,
    'SOCI': 25,
    'VETE': 26,
    'MULT': 27,
}
api_key = 'bfa9e3a1fe8cd6b275c767d14d28e702'
s = 0
date = 1965

for subj in list(subject_dict):
    st = f"https://api.elsevier.com/content/search/scopus?apiKey={api_key}&query=SUBJAREA%28{subj}%29&date={date}"
    r = requests.get(st)
    root = fromstring(r)
    tree = ElementTree(root)
  
    total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
    s += total_results
    print(f"{subj} {total_results}")

print(s)

subject_dict['UND'] = 28
s = 0
for subj in range(1, 29):
    self.cur.execute(f"select count(*) from publication JOIN publication_subject on publication_id = publication.id where subject_id={subj}")
        
    publication_id = self.cur.fetchone()
    s += publication_id[0]
    print(f"{list(subject_dict)[subj - 1]} {publication_id[0]}")

print(s)