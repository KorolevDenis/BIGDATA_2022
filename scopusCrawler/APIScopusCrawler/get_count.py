from xml.etree.ElementTree import ElementTree, fromstring, tostring
import requests
from lxml import etree
import sys

hostname = 'localhost'
username = 'denis'
password = 'qw'
database = 'denis'

connection = psycopg2.connect("""
    host=rc1b-1l9sdomsnwb23zda.mdb.yandexcloud.net
    port=6432
    sslmode=verify-full
    dbname=denis
    user=denis
    password=bigdata_2022
    target_session_attrs=read-write
""")
connection.set_client_encoding('UTF8')
cur = connection.cursor()

prefix_dict = {'dc' : 'http://purl.org/dc/elements/1.1/', 'ns0' : 'http://www.w3.org/2005/Atom',
    'ns1' : 'http://a9.com/-/spec/opensearch/1.1/', 'ns2' : 'http://prismstandard.org/namespaces/basic/2.0/'}

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
    'MULT': 27
}
api_key = '0a0838c93fec897a07b23cd41333229a'
s = 0

headers =  {
    'Accept': 'application/xml'
}
file = open("counts.txt", "a")
for date in range(int(sys.argv[1]), int(sys.argv[2])):
    file.write(f'year {date}\n')
    s = 0
    for subj in list(subject_dict):
        st = f"https://api.elsevier.com/content/search/scopus?apiKey={api_key}&query=SUBJAREA%28{subj}%29&date={date}"
        r = requests.get(st, headers=headers)
        root = fromstring(r.content)
        tree = ElementTree(root)

        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        s += total_results
        file.write(f"{subj} {total_results}\n")

    st = f"https://api.elsevier.com/content/search/scopus?apiKey={api_key}&query=NOT+SUBJAREA%28{'%29+AND+NOT+SUBJAREA%28'.join(list(subject_dict))}%29&date={date}"
    r = requests.get(st, headers=headers)
    root = fromstring(r.content)
    tree = ElementTree(root)

    total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
    s += total_results
    file.write(f"UND {total_results}\n")
    file.write(f"total {s}\n")

file.close()
