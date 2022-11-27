import scrapy
import time
import calendar
import datetime
import requests
from scrapy import Request
from scrapy.crawler import CrawlerProcess
from scrapy.http import HtmlResponse
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import DontCloseSpider
from xml.etree.ElementTree import ElementTree, fromstring, tostring
import json 
import itertools 
from fake_useragent import UserAgent
from threading import Thread

from ..items import *

prefix_dict = {'dc' : 'http://purl.org/dc/elements/1.1/', 'ns0' : 'http://www.w3.org/2005/Atom', 
    'ns1' : 'http://a9.com/-/spec/opensearch/1.1/', 'ns2' : 'http://prismstandard.org/namespaces/basic/2.0/'}

countries = [
    'China',
    'Germany',
    'Japan',
    'France',
    'Canada',
    'Italy',
    'India',
    'Australia',
    'Spain',
    'Netherlands',
    'Brazil',
    'Switzerland',
    'Sweden',
    'Poland',
    'Taiwan',
    'Iran',
    'Turkey',
    'Belgium',
    'Denmark',
    'Israel',
    'Austria',
    'Finland',
    'Norway',
    'Mexico',
    'Portugal',
    'Malaysia',
    'Greece',
    'Singapore',
    'Egypt',
    'Argentina',
    'Ireland',
    'Indonesia',
    'Hungary',
    'Romania',
    'Thailand',
    'Pakistan',
    'Ukraine',
    'Chile',
    'Colombia',
    'Nigeria',
    'Slovakia',
]

months = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
          'August', 'September', 'October', 'November', 'December']
          
doctypes = ['ar', 'cp', 're', 'ch', 'no', 'le', 'bk', 'sh', 'ed', 'er', 'cr', 'tb', 'rp', 'dp', 'ab', 'bz']

oas = ['publisherfullgold', 'publisherhybridgold', 'publisherfree2read']

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

chars = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']

class ScopusSpider(scrapy.Spider):
    name = 'scopus_api'
    
    def __init__(self, domains=None, name=None, *args, **kwargs):
        kwargs.pop('_job')
        super(ScopusSpider, self).__init__(*args, **kwargs)
        self.domains = json.loads(domains)

        file = open('apis.txt',mode='r')
        self.api_keys = file.read().replace("\r", "").split("\n")
        file.close()

        
    def start_requests(self):
        self.headers =  {
            'Accept': 'application/xml'
        }
        
        self.ua = UserAgent()
        self.headers['User-Agent'] = self.ua.random
        
        self.results = 0
        self.dates = ['1980', '1990', '2000', '2006', '2010', '2014', '2018', '2018', '2020', '2022']

        self.articles_count = 0
        self.proc_count = int(self.domains[1])
        self.proc_number = int(self.domains[2])
        self.api_key = self.api_keys[self.proc_number].replace("\n", "")
        date = self.domains[0]
        self.subject_dict_with_UND = subject_dict
        self.subject_dict_with_UND['UND'] = 28
        
        self.pages = [str(i) for i in range(360)]
        
        for subj in list(subject_dict):
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query=SUBJAREA%28{subj}%29&date={date}",
                    headers = self.headers, callback = self.parse_cited_by_helper, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"SUBJAREA%28{subj}%29"}, dont_filter=True)
        
        yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query=NOT+SUBJAREA%28{'%29+AND+NOT+SUBJAREA%28'.join(list(subject_dict))}%29&date={date}",
            headers = self.headers, callback = self.parse_start, errback=self.errback, meta = {"subj": 'UND', "date": date,
            "query": f"NOT+SUBJAREA%28{'%29+AND+NOT+SUBJAREA%28'.join(list(subject_dict))}%29"}, dont_filter=True)
        
        
    def errback(self, failure):
        status = failure.value.response.status
        self.logger.info('failureeeeeeeee errback %s %s', status ,failure.request.url)
        
        request = failure.request
        date = request.meta['date']

        apiKey_find = request.url.find('?apiKey=') + 8
        query_find = request.url.find('&query')
        
        self.logger.info(request.url[:apiKey_find] + self.api_key + request.url[query_find:])
        
        if status == 429:
            api_key = request.url[apiKey_find: query_find]
            
            if api_key != self.api_key:
                self.logger.info('api_key != self.api_key')
                yield Request(request.url[:apiKey_find] + self.api_key + request.url[query_find:], 
                        headers = request.headers, callback = request.callback, errback=self.errback, meta = request.meta, dont_filter=True)
                return
            
            res = requests.get(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query=all&date={date}")

            if 'x-ratelimit-remaining' in res.headers:
                quota = int(res.headers['x-ratelimit-remaining'])
            else:
                quota = 0
            self.logger.info(f"quota {quota}") 
            if quota == 0:
                res = requests.get(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query=all&date={date}")
                self.logger.info(f"res {res}")
                
                if 'x-ratelimit-remaining' in res.headers:
                    quota = int(res.headers['x-ratelimit-remaining'])
                else:
                    quota = 0
                self.logger.info(f"quota {quota}") 
                if quota == 0:
                    self.logger.info('quota == 0')
                    self.proc_number = (self.proc_number + self.proc_count) % len(self.api_keys)

                    self.api_key = self.api_keys[self.proc_number].replace("\n", "")
                    
                    yield Request(request.url[:apiKey_find] + self.api_key + request.url[query_find:], 
                        headers = request.headers, callback = request.callback, errback=self.errback, meta = request.meta, dont_filter=True)
                else:
                    yield Request(request.url[:apiKey_find] + self.api_key + request.url[query_find:], 
                        headers = request.headers, callback = request.callback, errback=self.errback, meta = request.meta, dont_filter=True)
                        
            else:
                yield Request(request.url[:apiKey_find] + self.api_key + request.url[query_find:], 
                    headers = request.headers, callback = request.callback, errback=self.errback, meta = request.meta, dont_filter=True)
        else:
            time.sleep(3)
            headers = request.headers
            headers['User-Agent'] = self.ua.random
            
            yield Request(request.url[:apiKey_find] + self.api_key + request.url[query_find:], 
                 headers = headers, callback = request.callback, errback=self.errback, meta = request.meta, dont_filter=True)
                

    def parse_doctype(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        subj = response.meta['subj']
        date = response.meta['date']
      
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_doctype total {total_results} url {url}")
        #self.logger.info(f"header {response.request.headers.get('User-Agent')}")
        response.request.headers['User-Agent'] = self.ua.random
        
        if total_results == 0:
            return
         
        if total_results <= 5000:
            yield from self.parse_start(response)
        else:
            for doctype in doctypes:
                yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+DOCTYPE%28{doctype}%29&date={date}",
                    headers = response.request.headers, callback = self.parse_access, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{query}+AND+DOCTYPE%28+{doctype}+%29"},
                    dont_filter=True)
                    
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+NOT+DOCTYPE%28{'%29+AND+NOT+DOCTYPE%28'.join(doctypes)}%29&date={date}",
                headers = response.request.headers, callback = self.parse_access, errback=self.errback, meta = {"subj": subj, 
                "date": date, "query": f"{query}+NOT+DOCTYPE%28{'%29+AND+NOT+DOCTYPE%28'.join(doctypes)}%29"},
                dont_filter=True)  
        
    def parse_cited_by_helper(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        subj = response.meta['subj']
        
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_cited_by_helper total {total_results} url {url}")
        #self.logger.info(f"header {response.request.headers.get('User-Agent')}")
        response.request.headers['User-Agent'] = self.ua.random
        
        if total_results == 0:
            return

        if total_results <= 5000:
            yield from self.parse_start(response)
        else:
            subj_id = subject_dict[subj]
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}&date={date}&sort=citedby-count",
                headers = response.request.headers, callback = self.parse, errback=self.errback, meta = {"subj_id": subj_id, "date": date, "query": ""}, dont_filter=True)  
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}&date={date}&sort=citedby-count&start=25",
                headers = response.request.headers, callback = self.parse, errback=self.errback, meta = {"subj_id": subj_id, "date": date, "query": ""}, dont_filter=True)    
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}&date={date}&sort=citedby-count&start=50",
                headers = response.request.headers, callback = self.parse_cited_by, errback=self.errback, meta = {"subj": subj, "date": date, "query": query}, dont_filter=True)     


    def parse_cited_by(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        subj = response.meta['subj']

        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_cited_by total {total_results} url {url}")
        #self.logger.info(f"header {response.request.headers.get('User-Agent')}")
        response.request.headers['User-Agent'] = self.ua.random
        
        if total_results == 0:
            return

        if total_results <= 5000:
            yield from self.parse_start(response)
        else:
            entry = tree.find(f".//{{{prefix_dict['ns0']}}}entry")
            citedby_count = int(entry.find(f".//{{{prefix_dict['ns0']}}}citedby-count").text)
            
            if citedby_count != 0:
                if citedby_count > 200:
                    step = citedby_count // 200
            
                    for volume_ind in range(0, step + 1):
                        if volume_ind == step:
                            right = citedby_count + 1
                        else:
                            right = (volume_ind + 1) * 200
                            
                        str_lhs = f"CITEDBY%28{'+OR+'.join([str(i) for i in range(volume_ind * 200, right)])}%29"
                        
                        yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_lhs}&date={date}",
                            headers = response.request.headers, callback = self.parse_cited_by_binary_rec, errback=self.errback,
                            meta = {"subj": subj, "date": date, "lhs_count": volume_ind * 200, "rhs_count": right, "query": query}, dont_filter=True)
                 
                else:
                    if citedby_count == 2:
                        midle = int(citedby_count // 2)
                    else:  
                        midle = int(citedby_count // 3)
                        
                    if citedby_count == 1:
                        str_lhs = "CITEDBY%280%29"
                        str_rhs = "CITEDBY%281%29"
                        
                        yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_lhs}&date={date}",
                            headers = response.request.headers, callback = self.parse_doctype, errback=self.errback, meta = {"subj": subj, "date": date, 
                            "query": f"{query}+AND+{str_lhs}"}, dont_filter=True)
                        
                        yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_rhs}&date={date}",
                            headers = response.request.headers, callback = self.parse_doctype, errback=self.errback, meta = {"subj": subj, "date": date, 
                            "query": f"{query}+AND+{str_rhs}"}, dont_filter=True)   
                            
                    else:  
                        str_lhs = f"CITEDBY%28{'+OR+'.join([str(i) for i in range(0, midle)])}%29"
                        str_rhs = f"CITEDBY%28{'+OR+'.join([str(i) for i in range(midle, citedby_count + 1)])}%29"
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_lhs}&date={date}",
                        headers = response.request.headers, callback = self.parse_cited_by_binary_rec, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": 0, 
                        "rhs_count": midle, "query": query}, dont_filter=True)
                        
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_rhs}&date={date}",
                        headers = response.request.headers, callback = self.parse_cited_by_binary_rec, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": midle, 
                        "rhs_count": citedby_count + 1, "query": query}, dont_filter=True)     
            else:
                yield Request(url,
                    headers = response.request.headers, callback = self.parse_doctype, errback=self.errback, meta = {"subj": subj, "date": date, 
                    "query": query}, dont_filter=True)
                
        
    def parse_cited_by_binary_rec(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        subj = response.meta['subj']
        
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_cited_by_binary_rec total {total_results} url {url}")
        #self.logger.info(f"header {response.request.headers.get('User-Agent')}")
        response.request.headers['User-Agent'] = self.ua.random
        
        if total_results == 0:
            return
     
        if total_results <= 5000:
            yield from self.parse_start(response)
        else:

            if 'lhs_count' in response.meta:               
                midle = int(response.meta['lhs_count'] + (response.meta['rhs_count'] - response.meta['lhs_count']) // 3)
                
                if response.meta['rhs_count'] - response.meta['lhs_count'] == 2:      
                    str_lhs = f"CITEDBY%28{response.meta['lhs_count']}%29"
                    str_rhs = f"CITEDBY%28{response.meta['lhs_count'] + 1}%29"
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_lhs}&date={date}",
                        headers = response.request.headers, callback = self.parse_doctype, errback=self.errback, meta = {"subj": subj, "date": date,
                        "query": f"{query}+AND+{str_lhs}"}, dont_filter=True)  
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_rhs}&date={date}",
                        headers = response.request.headers, callback = self.parse_doctype, errback=self.errback, meta = {"subj": subj, "date": date,
                        "query": f"{query}+AND+{str_rhs}"}, dont_filter=True)  
                        
                #elif  response.meta['rhs_count'] - response.meta['lhs_count'] == 1:
                #    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+CITEDBY%28{response.meta['lhs_count']}%29&date={date}",
                #        headers = self.headers, callback = self.parse_access, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{query}+AND+{str_lhs}"}, dont_filter=True)  
                        
                else:
                    str_lhs = f"CITEDBY%28{'+OR+'.join([str(i) for i in range(response.meta['lhs_count'], midle)])}%29"
                    str_rhs = f"CITEDBY%28{'+OR+'.join([str(i) for i in range(midle, response.meta['rhs_count'])])}%29"

                    lhs_callback = self.parse_cited_by_binary_rec
                    lhs_query = query
                    if midle - response.meta['lhs_count'] == 1:
                        lhs_callback = self.parse_doctype
                        lhs_query = f"{query}+AND+{str_lhs}"
                       
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_lhs}&date={date}",
                        headers = response.request.headers, callback = lhs_callback, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": response.meta['lhs_count'], 
                        "rhs_count": midle, "query": lhs_query}, dont_filter=True)
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_rhs}&date={date}",
                        headers = response.request.headers, callback = self.parse_cited_by_binary_rec, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": midle, 
                        "rhs_count": response.meta['rhs_count'], "query": query}, dont_filter=True)     
                        
    
    def parse_access(self, response, cited = None, rhs_cited = None, volume = False):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        subj = response.meta['subj']

        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_access total {total_results} url {url}")
        #self.logger.info(f"header {response.request.headers.get('User-Agent')}")
        response.request.headers['User-Agent'] = self.ua.random
        
        if total_results == 0:
            return
         
        if total_results <= 5000:
            yield from self.parse_start(response)
        else:
            str_access = lambda access: f"{query}+AND+OPENACCESS%28{access}%29"
                
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_access(1)}&date={date}",
                headers = response.request.headers, callback = self.parse_oas, errback=self.errback, meta = {"subj": subj, "date": date,
                "query": str_access(1)}, dont_filter=True)
            
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_access(0)}&date={date}",
                headers = response.request.headers, callback = self.parse_page, errback=self.errback, meta = {"subj": subj, "date": date, 
                "query": str_access(0)}, dont_filter=True)
                
            str_lhs = f"+AND+NOT+AUTHFIRST%28{'%29+AND+NOT+AUTHFIRST%28'.join(chars)}%29"
            #str_lhs = f"+AND+NOT+AUTHFIRST%28{'%29+AND+NOT+AUTHFIRST%28'.join(chars)}%29"

            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}{str_lhs}&date={date}&sort=citedby-count",
                headers = self.headers, callback = self.parse_start, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{query}{str_lhs}"},
                dont_filter=True)

  
    def parse_oas(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        subj = response.meta['subj']

        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_oas total {total_results} url {url}")
        #self.logger.info(f"header {response.request.headers.get('User-Agent')}")
        response.request.headers['User-Agent'] = self.ua.random
        
        if total_results == 0:
            return
         
        if total_results <= 5000:
            yield from self.parse_start(response)
        else:
            for oa in oas:
                yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+OA%28{oa}%29&date={date}",
                    headers = response.request.headers, callback = self.parse_page, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{query}+AND+OA%28{oa}%29"}, dont_filter=True)
                    
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+NOT+OA%28{'+OR+'.join(oas)}%29&date={date}",
                    headers = response.request.headers, callback = self.parse_page, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{query}+AND+NOT+OA%28{'+OR+'.join(oas)}%29"}, 
                    dont_filter=True)

   
    def parse_page(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        subj = response.meta['subj']

        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_page url {total_results} {url}  ")
        #self.logger.info(f"header {response.request.headers.get('User-Agent')}")
        response.request.headers['User-Agent'] = self.ua.random
        
        if total_results == 0:
            return
         
        if total_results <= 5000:
            yield from self.parse_start(response)
        else:
            str_lhs = f"PAGEFIRST%28{'+OR+'.join(self.pages)}%29"
            str_rhs = f"NOT+PAGEFIRST%28{'+OR+'.join(self.pages)}%29"

            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_lhs}&date={date}",
                headers = response.request.headers, callback = self.parse_page_rec, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": 0, 
                "rhs_count": len(self.pages), "query": query}, dont_filter=True)
                
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_rhs}&date={date}",
                headers = response.request.headers, callback = self.parse_mouth, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{query}+AND+{str_rhs}"}, dont_filter=True)


    def parse_page_rec(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        subj = response.meta['subj']

        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_page_by_binary_rec total {total_results} url {url}")
        #self.logger.info(f"header {response.request.headers.get('User-Agent')}")
        response.request.headers['User-Agent'] = self.ua.random
        
        if total_results == 0:
            return
     
        if total_results <= 5000:
            yield from self.parse_start(response)
        else:
            if 'lhs_count' in response.meta:
                midle = int(response.meta['lhs_count'] + (response.meta['rhs_count'] - response.meta['lhs_count']) // 2)
                                   
                if response.meta['rhs_count'] - response.meta['lhs_count'] == 2:
                    str_lhs = f"PAGEFIRST%28{response.meta['lhs_count']}%29"
                    str_rhs = f"PAGEFIRST%28{response.meta['lhs_count'] + 1}%29" 
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_lhs}&date={date}",
                        headers = response.request.headers, callback = self.parse_mouth, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{query}+AND+{str_lhs}"}, 
                        dont_filter=True)
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_rhs}&date={date}",
                        headers = response.request.headers, callback = self.parse_mouth, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{query}+AND+{str_rhs}"},
                        dont_filter=True)   

                else:
                    str_lhs = f"PAGEFIRST%28{'+OR+'.join(self.pages[response.meta['lhs_count'] : midle])}%29"
                    str_rhs = f"PAGEFIRST%28{'+OR+'.join(self.pages[midle : response.meta['rhs_count']])}%29"

                    lhs_callback = self.parse_page_rec
                    lhs_query = query
                    if midle - response.meta['lhs_count'] == 1:
                        lhs_callback = self.parse_mouth
                        lhs_query = f"{query}+AND+{str_lhs}"
                
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_lhs}&date={date}",
                        headers = response.request.headers, callback = lhs_callback, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": response.meta['lhs_count'], 
                        "rhs_count": midle, "query": lhs_query}, dont_filter=True)
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_rhs}&date={date}",
                        headers = response.request.headers, callback = self.parse_page_rec, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": midle,
                        "rhs_count": response.meta['rhs_count'], "query": query}, dont_filter=True)   
   
   
    def parse_mouth(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        subj = response.meta['subj']

        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_page url {total_results} {url}  ")
        #self.logger.info(f"header {response.request.headers.get('User-Agent')}")
        response.request.headers['User-Agent'] = self.ua.random
        
        if total_results == 0:
            return
         
        if total_results <= 5000:
            yield from self.parse_start(response)
        else:
            midle = len(months) // 2
            
            str_rhs = f"NOT+PUBDATETXT%28{'+OR+'.join(months)}%29"
            
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query=PUBDATETXT%28{'+OR+'.join(months[:midle])}%29+AND+{query}&date={date}",
                headers = response.request.headers, callback = self.parse_mouth_rec, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": 0, 
                "rhs_count": midle, "query": query}, dont_filter=True)
            
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query=PUBDATETXT%28{'+OR+'.join(months[midle:])}%29+AND+{query}&date={date}",
                headers = response.request.headers, callback = self.parse_mouth_rec, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": midle, 
                "rhs_count": len(months), "query": query}, dont_filter=True)
                
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+NOT+PUBDATETXT%28{date}%29&date={date}",
                headers = response.request.headers, callback = self.parse_countries, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{query}+AND+{str_rhs}"},
                dont_filter=True)
                
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_rhs}&date={date}",
                headers = response.request.headers, callback = self.parse_countries, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{query}+AND+{str_rhs}"}, 
                dont_filter=True)


    def parse_mouth_rec(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        subj = response.meta['subj']
        
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_mouth_rec total {total_results} url {url}")
        #self.logger.info(f"header {response.request.headers.get('User-Agent')}")
        response.request.headers['User-Agent'] = self.ua.random
        
        if total_results == 0:
            return
     
        if total_results <= 5000:
            yield from self.parse_start(response)
        else:
            if 'lhs_count' in response.meta:
                midle = int(response.meta['lhs_count'] + (response.meta['rhs_count'] - response.meta['lhs_count']) // 3)
                                   
                if response.meta['rhs_count'] - response.meta['lhs_count'] == 2:
                    str_lhs = f"PUBDATETXT%28{months[response.meta['lhs_count']]}%29"
                    str_rhs = f"PUBDATETXT%28{months[response.meta['lhs_count'] + 1]}%29" 
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_lhs}+AND+{query}&date={date}",
                        headers = response.request.headers, callback = self.parse_countries, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{str_lhs}+AND+{query}"}, dont_filter=True)
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_rhs}+AND+{query}&date={date}",
                        headers = response.request.headers, callback = self.parse_countries, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{str_rhs}+AND+{query}"}, dont_filter=True)   
 
                else:
                    str_lhs = f"PUBDATETXT%28{'+OR+'.join(months[response.meta['lhs_count'] : midle])}%29"
                    str_rhs = f"PUBDATETXT%28{'+OR+'.join(months[midle : response.meta['rhs_count']])}%29"

                    lhs_callback = self.parse_mouth_rec
                    lhs_query = query
                    if midle - response.meta['lhs_count'] == 1:
                        lhs_callback = self.parse_countries
                        lhs_query = f"{str_lhs}+AND+{query}"
                
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_lhs}+AND+{query}&date={date}",
                        headers = response.request.headers, callback = lhs_callback, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": response.meta['lhs_count'], 
                        "rhs_count": midle, "query": lhs_query}, dont_filter=True)
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_rhs}+AND+{query}&date={date}",
                        headers = response.request.headers, callback = self.parse_mouth_rec, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": midle,
                        "rhs_count": response.meta['rhs_count'], "query": query}, dont_filter=True) 

                        
    def parse_countries(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        subj = response.meta['subj']
        
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_countries url {total_results} {url}  ")
        #self.logger.info(f"header {response.request.headers.get('User-Agent')}")
        response.request.headers['User-Agent'] = self.ua.random
        
        if total_results == 0:
            return
         
        if total_results <= 5000:
            yield from self.parse_start(response)
        else: 
            str_lhs = f"AFFILCOUNTRY%28{'+OR+'.join(countries)}%29"
            str_rhs = f"NOT+AFFILCOUNTRY%28{'+OR+'.join(countries)}%29"
            
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_lhs}+AND+{query}&date={date}",
                headers = response.request.headers, callback = self.parse_countries_rec, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": 0, 
                "rhs_count": len(countries), "query": query}, dont_filter=True)
                
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={query}+AND+{str_rhs}&date={date}",
                headers = response.request.headers, callback = self.parse_authfirst, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{query}+AND+{str_rhs}"}, dont_filter=True)
               

    def parse_countries_rec(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        subj = response.meta['subj']
        
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_countries_rec total {total_results} url {url}")
        #self.logger.info(f"header {response.request.headers.get('User-Agent')}")
        response.request.headers['User-Agent'] = self.ua.random
        
        if total_results == 0:
            return
     
        if total_results <= 5000:
            yield from self.parse_start(response)
        else:
            if 'lhs_count' in response.meta:
                midle = int(response.meta['lhs_count'] + (response.meta['rhs_count'] - response.meta['lhs_count']) // 3)
                                   
                if response.meta['rhs_count'] - response.meta['lhs_count'] == 2:
                    str_lhs = f"AFFILCOUNTRY%28{countries[response.meta['lhs_count']]}%29"
                    str_rhs = f"AFFILCOUNTRY%28{countries[response.meta['lhs_count'] + 1]}%29"                       
                             
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_lhs}+AND+{query}&date={date}",
                        headers = response.request.headers, callback = self.parse_authfirst, errback=self.errback, meta = {"subj": subj, "date": date, 
                        "query": f"{str_lhs}+AND+{query}"}, dont_filter=True)
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_rhs}+AND+{query}&date={date}",
                        headers = response.request.headers, callback = self.parse_authfirst, errback=self.errback, meta = {"subj": subj, "date": date, 
                        "query": f"{str_rhs}+AND+{query}"}, dont_filter=True)   
 
                else:
                    str_lhs = f"AFFILCOUNTRY%28{'+OR+'.join(countries[response.meta['lhs_count']: midle])}%29"
                    str_rhs = f"AFFILCOUNTRY%28{'+OR+'.join(countries[midle: response.meta['rhs_count']])}%29"
                    
                    lhs_callback = self.parse_countries_rec
                    lhs_query = query
                    if midle - response.meta['lhs_count'] == 1:
                        lhs_callback = self.parse_authfirst
                        lhs_query = f"{str_lhs}+AND+{query}"
                
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_lhs}+AND+{query}&date={date}",
                        headers = response.request.headers, callback = lhs_callback, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": response.meta['lhs_count'], 
                        "rhs_count": midle, "query": lhs_query}, dont_filter=True)
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_rhs}+AND+{query}&date={date}",
                        headers = response.request.headers, callback = self.parse_countries_rec, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": midle,
                        "rhs_count": response.meta['rhs_count'], "query": query}, dont_filter=True)   
                 
                        
    def parse_authfirst(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        subj = response.meta['subj']
        
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_authfirst total {total_results} url {url}")
        #self.logger.info(f"header {response.request.headers.get('User-Agent')}")
        response.request.headers['User-Agent'] = self.ua.random
        
        if total_results == 0:
            return
        
        if total_results <= 5000:
            yield from self.parse_start(response)
        else:
            midle = int(len(chars) // 2)
            str_lhs = f"AUTHFIRST%28{'+OR+'.join(chars[0 : midle])}%29"
            str_rhs = f"AUTHFIRST%28{'+OR+'.join(chars[midle: len(chars)])}%29"
                
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_lhs}+AND+{query}&date={date}",
                headers = response.request.headers, callback = self.parse_authfirst_rec, errback=self.errback, meta = {"subj": subj, "date": date, 
                "lhs_count": 0, "rhs_count": midle, "query": query}, dont_filter=True)
                    
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_rhs}+AND+{query}&date={date}",
                headers = response.request.headers, callback = self.parse_authfirst_rec, errback=self.errback, meta = {"subj": subj, "date": date, 
                "lhs_count": midle, "rhs_count": len(chars), "query": query}, dont_filter=True)
        
            
    def parse_authfirst_rec(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        subj = response.meta['subj']
        
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_authfirst_rec url {total_results} {url}")
        #self.logger.info(f"header {response.request.headers.get('User-Agent')}")
        response.request.headers['User-Agent'] = self.ua.random
        
        if total_results == 0:
            return
                 
        if total_results <= 5000:
            yield from self.parse_start(response)
        else:
            if 'lhs_count' in response.meta:
                midle = int(response.meta['lhs_count'] + (response.meta['rhs_count'] - response.meta['lhs_count']) // 2)
        
                if response.meta['rhs_count'] - response.meta['lhs_count'] == 2:
                    str_lhs = f"AUTHFIRST%28{chars[response.meta['lhs_count']]}%29"
                    str_rhs = f"AUTHFIRST%28{chars[response.meta['lhs_count'] + 1]}%29"
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_lhs}+AND+{query}&date={date}",
                        headers = response.request.headers, callback = self.parse_start, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{str_lhs}+AND+{query}"}, dont_filter=True)
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_rhs}+AND+{query}&date={date}",
                        headers = response.request.headers, callback = self.parse_start, errback=self.errback, meta = {"subj": subj, "date": date, "query": f"{str_rhs}+AND+{query}"}, dont_filter=True)   
                        
                else:  
                    str_lhs = f"AUTHFIRST%28{'+OR+'.join(chars[response.meta['lhs_count'] : midle])}%29"
                    str_rhs = f"AUTHFIRST%28{'+OR+'.join(chars[midle : response.meta['rhs_count']])}%29"
                    
                    lhs_callback = self.parse_authfirst_rec
                    lhs_query = query
                    if midle - response.meta['lhs_count'] == 1:
                        lhs_callback = self.parse_start
                        lhs_query = f"{str_lhs}+AND+{query}"
                        
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_lhs}+AND+{query}&date={date}",
                        headers = response.request.headers, callback = lhs_callback, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": response.meta['lhs_count'],
                        "rhs_count": midle, "query": lhs_query}, dont_filter=True)
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={self.api_key}&query={str_rhs}+AND+{query}&date={date}",
                        headers = response.request.headers, callback = self.parse_authfirst_rec, errback=self.errback, meta = {"subj": subj, "date": date, "lhs_count": midle, 
                        "rhs_count": response.meta['rhs_count'], "query": query}, dont_filter=True)  


    def parse_start(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query = response.meta['query']
        subj = response.meta['subj']
        date = response.meta['date']
        
        response.request.headers['User-Agent'] = self.ua.random       
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)           
        self.logger.info(f"parse_start {url}")   
        
        begin = 25
        
        if start_pos := url.find("&start=") != -1:
            begin = int(url[url.find("&start=") + 7 : len(url)])
            url = url[:url.find("&start=")]
            
        if total_results == 0:
            return
        
        subj_id = self.subject_dict_with_UND[subj]
        if total_results <= 5000:
            #self.results += total_results
            #self.logger.info(self.results)
            yield from self.parse(response, subj_id)
            for start in range(begin, ((total_results - 1) // 25 + 1) * 25, 25):
                yield Request(f"{url}&start={start}",
                    headers = response.request.headers, callback = self.parse, errback=self.errback, meta = {"subj_id": subj_id, "date": date})
        else:
            self.results += total_results
            self.logger.info(f"more {self.results}")
     

    def parse(self, response, subj_id = None):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        date = response.meta['date']
        
        if subj_id is None:
            subj_id = response.meta['subj_id']

        articles = tree.findall(f".//{{{prefix_dict['ns0']}}}entry")
        end = len(articles)
        
        if 'parse_end' in response.meta:
            end = response.meta['parse_end']
           
        #TODO убрать
        #self.articles_count += end
        #self.logger.info(f"articles_count {self.articles_count}")
        
        for article in range(0, end):
            article_item = dict()           
            result = dict()
            result['url'] = url
            result['affiliations'] = []

            for affiliation in articles[article].findall(f".//{{{prefix_dict['ns0']}}}affiliation"):
                affiliation_item = APIAffiliationItem()
                affiliation_item['name'] = getattr(affiliation.find(f"./{{{prefix_dict['ns0']}}}affilname"), 'text', None)
                affiliation_item['city'] = getattr(affiliation.find(f"./{{{prefix_dict['ns0']}}}affiliation-city"), 'text', None)
                affiliation_item['country'] = getattr(affiliation.find(f"./{{{prefix_dict['ns0']}}}affiliation-country"), 'text', None)

                result['affiliations'].append(affiliation_item)
                
            journal_item = APIJournalItem()
            journal_item['name'] = getattr(articles[article].find(f"./{{{prefix_dict['ns2']}}}publicationName"), 'text', None)
            journal_item['issn'] = getattr(articles[article].find(f"./{{{prefix_dict['ns2']}}}issn"), 'text', None)
            journal_item['e_issn'] = getattr(articles[article].find(f"./{{{prefix_dict['ns2']}}}eIssn"), 'text', None)
            journal_item['aggregation_type'] = getattr(articles[article].find(f"./{{{prefix_dict['ns2']}}}aggregationType"), 'text', None)
            
            article_item['log_date'] = date
            article_item['link_scopus'] = articles[article].find(f"./{{{prefix_dict['ns0']}}}link[@ref='scopus']").attrib["href"]
            article_item['link_citedby'] = articles[article].find(f"./{{{prefix_dict['ns0']}}}link[@ref='scopus-citedby']").attrib["href"]
            
            article_item['scopus_id'] = getattr(articles[article].find(f"./{{{prefix_dict['dc']}}}identifier"), 'text', None).replace("SCOPUS_ID:", "")
            article_item['eid'] = getattr(articles[article].find(f"./{{{prefix_dict['ns0']}}}eid"), 'text', None)
            article_item['title'] = getattr(articles[article].find(f"./{{{prefix_dict['dc']}}}title"), 'text', None)
      
            article_item['volume'] = getattr(articles[article].find(f"./{{{prefix_dict['ns2']}}}volume"), 'text', None)
            article_item['subject_id'] = subj_id
            
            article_item['raw_xml'] = tostring(articles[article], encoding='unicode')
            article_item['cur_time'] = datetime.datetime.now()
            article_item['api_key'] = self.api_key
        
            article_item['issue_identifier'] = getattr(articles[article].find(f"./{{{prefix_dict['ns2']}}}issueIdentifier"), 'text', None)
            #if issue_identifier is None:
            #    article_item['issue_identifier'] = ""
            #else:
            #    article_item['issue_identifier'] = issue_identifier.text
               
            article_item['citedby_count'] = getattr(articles[article].find(f"./{{{prefix_dict['ns0']}}}citedby-count"), 'text', None)
            #if citedby_count is None:
            #    article_item['citedby_count'] = 0
            #else:
            #    article_item['citedby_count'] = citedby_count.text 

            article_item['page_range'] = getattr(articles[article].find(f"./{{{prefix_dict['ns2']}}}pageRange"), 'text', None)
            #if page_range is None:
            #    article_item['page_range'] = ""
            #else:
            #    article_item['page_range'] = page_range

            article_item['cover_date'] = getattr(articles[article].find(f"./{{{prefix_dict['ns2']}}}coverDate"), 'text', None)
            article_item['doi'] = getattr(articles[article].find(f"./{{{prefix_dict['ns2']}}}doi"), 'text', None)
            article_item['description'] = ""
            
            article_item['pubmed_id'] = getattr(articles[article].find(f"./{{{prefix_dict['ns0']}}}pubmed-id"), 'text', None)
            article_item['subtype'] = getattr(articles[article].find(f"./{{{prefix_dict['ns0']}}}subtype"), 'text', None)
            article_item['article_number'] = getattr(articles[article].find(f"./{{{prefix_dict['ns0']}}}article-number"), 'text', None)
            article_item['source_id'] = getattr(articles[article].find(f"./{{{prefix_dict['ns0']}}}source-id"), 'text', None)
            article_item['openaccess'] = getattr(articles[article].find(f"./{{{prefix_dict['ns0']}}}openaccess"), 'text', "f")
            
            result['freetoread'] = []
            result['freetoread_label'] = []
                            
            if article_item['openaccess']:
                freetoread = articles[article].find(f".//{{{prefix_dict['ns0']}}}freetoread")
                if(freetoread is not None):
                    for value in freetoread.findall(f".//{{{prefix_dict['ns0']}}}value"):
                        result['freetoread'].append(value.text)
                        
                freetoread_label = articles[article].find(f".//{{{prefix_dict['ns0']}}}freetoreadLabel")   
                if(freetoread_label is not None):
                    for value in freetoread_label.findall(f".//{{{prefix_dict['ns0']}}}value"):
                        result['freetoread_label'].append(value.text)

            result['publication'] = article_item
            result['author'] = getattr(articles[article].find(f"./{{{prefix_dict['dc']}}}creator"), 'text', None)
            result['journal'] = journal_item
            yield result

            

