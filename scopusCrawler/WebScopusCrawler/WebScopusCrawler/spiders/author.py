from selenium import webdriver
from webdriver_manager.firefox import GeckoDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
import re
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from pyshadow.main import Shadow
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from selenium.webdriver.common.proxy import Proxy, ProxyType
import psycopg2
import sys
from threading import Thread, active_count

COOKIES = dict([l.split("=", 1) for l in chrome_cookies.replace(" ", "").split(";")])
USERNAME = ''
PASSWORD = ''
INSTITUTION = 'Санкт-Петербургский политехнический университет Петра Великого'

WAIT_TIME = 30
# Define the time limit to downloading:
DOWNLOAD_TIMEOUT = 90
# Define the sleep time for waiting for the rendering (of HTML/JavaScript):
# (If necessary, please prolong the sleep time,
# especially when your network is slowed down by the Great Firewall in Mainland China)
SLEEPTIME_LONG = 10  # Generally for waiting for redirecting/loading of the search page of Scopus
SLEEPTIME_MEDIUM = 5  # Generally for waiting for interacting with elements shown via rendering of JavaScript
SLEEPTIME_SHORT = 2  # Generally for waiting for interacting with elements shown via rendering of HTML
# Times limit to trying (to download) again:
TRY_AGAIN_TIMES = 5

class A:
    def __init__(self):
        try:
            myProxy = "8.219.97.248:80"

            proxy = Proxy({
                'proxyType': ProxyType.MANUAL,
                'httpProxy': myProxy,
                'ftpProxy': myProxy,
                'sslProxy': myProxy,
                'noProxy': '' # set this value as desired
                })          

            options = webdriver.FirefoxOptions()
            ua = UserAgent()
            user_agent = ua.random
            #caps = DesiredCapabilities().FIREFOX
            #caps["pageLoadStrategy"] = "eager"  #  interactive
            options.add_argument(f'user-agent="{user_agent}"')
            #options.add_experimental_option('excludeSwitches', ['enable-logging'])
            #options.add_argument('--headless')
            #options.add_argument('--sandbox')
            #options.add_argument('--disable-gpu')
            #options.set_preference("dom.webnotifications.serviceworker.enabled", False)
            #options.set_preference("dom.webnotifications.enabled", False)
            #self.driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), capabilities=caps, options=options, proxy=proxy)
            self.driver = webdriver.Remote(command_executor="http://localhost:4444/wd/hub",desired_capabilities=DesiredCapabilities.FIREFOX)
            self.wait = WebDriverWait(self.driver, WAIT_TIME)
            self.short_wait = WebDriverWait(self.driver, 5)
            self.shortest_wait = WebDriverWait(self.driver, 2)
  
            self.hostname = os.getenv('POSTGRES_HOST')
            self.username = os.getenv('POSTGRES_USER')
            self.password = os.getenv('POSTGRES_PASSWORD')
            self.database = os.getenv('POSTGRES_DB')

            self.connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database)
            self.connection.set_client_encoding('UTF8')
            self.cur = self.connection.cursor()
            self.log_in("pass")
            
            self.right_date = int(sys.argv[1])
            self.right_date = int(sys.argv[2])
            self.proc_count = int(sys.argv[3])
            self.proc_number = int(sys.argv[4])
            
            date = self.right_date - self.proc_number
            self.proc_count = 1
            try:
                count = self.get_request_count(date)
                print(f"count {count}")
                req_count = 0
                offset_count = 0
                rec_count = 100
                request_count = 0
                print("")
                self.cur.execute(f"""SELECT link_scopus, link_citedby, eid, publication_{date}.id FROM publication_{date} JOIN publication_author_{date} on 
                                publication_{date}.id = publication_id JOIN affiliation on affiliation_id = affiliation.id WHERE city LIKE '%Saint Petersburg%' or city LIKE '%Leningrad%' 
                                ORDER BY id LIMIT {rec_count};""")
                publications = self.cur.fetchall()
                
                print(f"active_count() {active_count()}")
                active_c = active_count()
                while offset_count * rec_count + req_count < count:          
                    self.parse(publications[req_count % rec_count], date)

                    req_count += 1

                    if req_count % rec_count == 0:
                        #print(f"in if {rec_count - active_count() + active_c}")
                        offset_count += 1
                        req_count = 0
                        
                        self.cur.execute(f"""SELECT link_scopus, link_citedby, eid, publication_{date}.id FROM publication_{date} JOIN publication_author_{date} on 
                                publication_{date}.id = publication_id JOIN affiliation on affiliation_id = affiliation.id WHERE city LIKE '%Saint Petersburg%' or city LIKE '%Leningrad%'
                                ORDER BY id LIMIT {rec_count} OFFSET {offset_count * rec_count};""")
                        publications = self.cur.fetchall()
                        
                    if offset_count * rec_count + req_count >= count:
                        count = self.get_request_count(date)
                                                    
                        while request_count < 1 and count == self.get_request_count(date):
                            count = self.get_request_count(date)
                            request_count += 1
                        
                        if request_count == 1:
                            request_count = 0
                            req_count = 0
                            offset_count = 0
                            
                            date = date - self.proc_count
                            if date < self.left_date:
                                return
                            print(f"date {date}")
                                                    
                        print(f"count {count}")
      
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)
            #self.driver.quit()    
            
            
    def get_request_count(self, date):
        self.cur.execute(f"""SELECT count(publication_{date}.id) FROM publication_{date} JOIN publication_author_{date} on 
                                publication_{date}.id = publication_id JOIN affiliation on affiliation_id = affiliation.id WHERE city LIKE '%Saint Petersburg%' or city LIKE '%Leningrad%';""")
        return self.cur.fetchone()[0]
        
        
    def log_in(self, type):
        try:
            if (type == "cockies"):
                print('Logging in via cookies ...')

                self.driver.get('https://www.scopus.com/search/form.uri?display=advanced')

                # Add cookies to the website of Scopus:
                print('-- Adding cookies ...')
                print(COOKIES)
                for name, value in COOKIES.items():
                    self.driver.add_cookie({
                        'name': name,
                        'value': value})

                print('-- Testing whether you successfully logged in ...')

                self.driver.get('https://www.scopus.com/search/form.uri?display=advanced')
                time.sleep(
                    SLEEPTIME_LONG)  # Please lengthen this waiting time, when your network can successfully log in manually but not via this program.
                judge = self.driver.current_url.count("advanced")
                print(judge)
                if judge > 0:
                    print('-- Successfully logged in!')
                if judge == 0:
                    print('-- Failed to log in.')
                    self.log_in(type)
            elif (type == "pass"):
                self.driver.get(
                    'https://id.elsevier.com/as/authorization.oauth2?platSite=SC%2Fscopus&ui_locales=en_US&scope=openid+profile+email+els_auth_info+els_analytics_info+urn%3Acom%3Aelsevier%3Aidp%3Apolicy%3Aproduct%3Ainst_assoc&response_type=code&redirect_uri=https%3A%2F%2Fwww.scopus.com%2Fauthredirect.uri%3FtxGid%3Dbc6f0c0ea70010df705a26ab6916511f&state=checkAccessLogin&authType=SINGLE_SIGN_IN&prompt=login&client_id=SCOPUS')
                print('-- Inputting, submitting, and choosing your institution ...')
                institution_input = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#bdd-email')))
                institution_submit = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '#bdd-els-searchBtn')))
                institution_input.send_keys(INSTITUTION)
                institution_submit.click()
                institution_choice = self.wait.until(EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, '#bdd-institution-resultList > form:nth-child(1) > button')))
                institution_choice.click()
                login_scopus = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '#bdd-elsPrimaryBtn')))
                login_scopus.click()

                # Redirect to the VPN of your institution. Input and submit your username and password:
                print(
                    '-- Redicrecting to the VPN of your institution. Inputting and submitting your username and password ...')
                try:
                    username_institution = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#user')))
                    password_institution = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#password')))
                    username_institution.send_keys(USERNAME)
                    password_institution.send_keys(PASSWORD)
                    submit_institution = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '#doLogin')))
                    submit_institution.click()
                except TimeoutException:
                    print('scip inst')
                    
                # Test whether you successfully logged in:
                print('-- Testing whether you successfully logged in ...')
                self.driver.get('https://www.scopus.com/search/form.uri?display=advanced')
                time.sleep(
                    20)  # Please lengthen this waiting time, when your network can successfully log in manually but not via this program.
                judge = self.driver.current_url.count("advanced")
                print(judge)
                if judge > 0:
                    print('-- Successfully logged in!')
                    return
                if judge == 0:
                    print('-- Failed to log in.')
                    self.log_in(type)        
        except Exception as e:
            print(e)
            print('Try again ...')
            self.log_in(type)


    def parse(self, publication, date):
        try:           
            result = dict()
            result[type] = 1
            self.driver.get(publication[0])
            print(publication[0])
            result['eid'] = publication[2]
            result['pub_id'] = publication[3]
            result['abstract'] = None
            result['keywords'] = None
            
            try:
                result['abstract'] = self.short_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'section.col-24.col-lg-18 p > span'))).text
            except Exception as e:
                print("result['abstract']")   
                print(e)   
                
            try:
                keywords = []
                self.short_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'section.col-24.col-lg-18 .margin-size-4-t > span')))
                for keyword in self.driver.find_elements(By.CSS_SELECTOR, 'section.col-24.col-lg-18 .margin-size-4-t > span'):
                    keywords.append(keyword.text)
                    
                result['keywords'] = '; '.join(keywords)
            except Exception as e:
                print("result['keywords']")  
                print(e)   

            result['authors'] = self.select_authors()
            result['log_date'] = date
            result['citations'] = list()
            print(f"link_count {len(result['citations'])}\n")
                
            #print(result) 
            self.process_item(result)
            return

        except Exception as e:
            print(e)
         
         
    def select_authors_and_cit(self, url_cited_by):
        authors = []
        citations = []
        
        try:
            count = None
            
            self.shortest_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'div.row > ul.refTitleCount.list-inline')))
            count_str = self.shortest_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'ul.refTitleCount.list-inline h4.subTitle'))).text
            
            if count_str == "":
                count = 0
            else:
                count = int(count_str[count_str.find('ки (') + 4: len(count_str) - 1])
                
        except Exception as e:
            print("no refTitleCount")
            print(e)
            authors = self.select_authors()
            citations = self.select_cited_by_search(url_cited_by, count)

            return authors, citations
    
        print(f"count {count}\n")
        if count > 80:
            try:
                href = self.driver.find_element(By.CSS_SELECTOR, 'ul.refTitleCount.list-inline > li.pull-right > a').get_attribute("href")
            except Exception:  
                print(f"no href\n")
                href = url_cited_by
            authors = self.select_authors()
            citations = self.select_cited_by_search(href, count)
        else:
            link_count = 0
            self.short_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'tbody.referencesUL')))
            links = self.driver.find_elements(By.CSS_SELECTOR, 'tbody.referencesUL > tr > td.refCont')
            print(f"links len {len(links)}\n")

            while link_count < len(links):
                citation = dict()
                
                try:
                    body = links[link_count].find_element(By.CSS_SELECTOR, 'div.refDocTitle.fontMedium')
                    href = body.find_element(By.CSS_SELECTOR, 'a')
                    scopus_link = href.get_attribute("href")
                    
                    citation['eid'] = scopus_link[scopus_link.find("eid=") + 4 : scopus_link.find("&", scopus_link.find("eid="), len(scopus_link))]
                    citation['title'] = href.text  
                    citation['scopus_link'] = scopus_link
                    citation['cited_by_link'] = links[link_count].find_element(By.CSS_SELECTOR , 'a').get_attribute("href")
                    citation['is_scopus'] = True
                    
                except Exception as e:
                    try:
                        body = links[link_count].find_element(By.CSS_SELECTOR, 'div.refAuthorTitle')
                        try:
                            cited_by_link = body.find_element(By.CSS_SELECTOR, 'a').get_attribute("href")
                        except NoSuchElementException:
                            cited_by_link = None
                            
                    except NoSuchElementException as e:
                        print(f"link_count {link_count}\n")
                        link_count += 1
                        continue
                                    
                    if cited_by_link is None or cited_by_link.find("refeid=") == -1:
                        citation['title'] = body.text
                        citation['eid'] = cited_by_link
                        citation['scopus_link'] = None
                        citation['cited_by_link'] = cited_by_link
                        citation['is_scopus'] = False
                    else:
                        citation['title'] = body.text.split("\n")[1]
                        citation['eid'] = cited_by_link[cited_by_link.find("eid=") + 7 : cited_by_link.find("&", cited_by_link.find("eid="), len(cited_by_link))]
                        citation['scopus_link'] = f"https://www.scopus.com/record/display.uri?eid={citation['eid']}&origin=reflist"
                        citation['cited_by_link'] = cited_by_link
                        citation['is_scopus'] = False
                
                #print(f"eid {citation['eid']}")
                #print(f"title {citation['title']}")
                #print(f"scopus_link {citation['scopus_link']}")
                #print(f"cited_by_link {citation['cited_by_link']}")
                #print(f"\n")
                
                citations.append(citation)
                link_count += 1
            authors = self.select_authors()
            
        return authors, citations
        
        
    def select_authors(self):
        authors = []

        try:
            author_li = self.short_wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 
                'div.row > div.col-24.col-lg-18.col-xl-16 > ul.ul--horizontal.margin-size-0 > li > els-button')))
        except Exception as e:
            return list()           
            
        author_li_len = len(author_li)
        if author_li_len == 0:
            return list()
            
        print(f"len {author_li_len}")
        for author_index in range(0, author_li_len):
            self.driver.execute_script(
                f"document.querySelectorAll('div.row > div.col-24.col-lg-18.col-xl-16 > ul.ul--horizontal.margin-size-0 > li > els-button')[{author_index}].shadowRoot.querySelector('button').click()")      
        
        #contents = self.driver.find_elements(By.XPATH, "//div[@class='content_77e6a8']")
        try:
            self.short_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'els-stack.stack--s')))
        except Exception as e:
            print("no els-stack.stack--s")
            return list()  
    
        #while len(self.driver.find_elements(By.CSS_SELECTOR, "els-stack.stack--s")) != author_li_len:
        #    time.sleep(0.001)

        contents = self.driver.find_elements(By.CSS_SELECTOR, "els-stack.stack--s")
        for ind, content in enumerate(contents):
            author = dict()
            try:
                author['name'] = content.find_element(By.CSS_SELECTOR, 'h1').text
                
                try:
                    auid = content.find_element(By.CSS_SELECTOR, 'els-stack > els-stack > div > els-stack > els-button.hydrated').get_attribute("href")
                    author['auid'] = auid[auid.find("authorId=") + 9 : auid.find("&origin")]
                except Exception as e:
                    author['auid'] = None

                author['affils'] = []
                affils = content.find_elements(By.CSS_SELECTOR, 'els-stack.stack.stack--m > span')
                for ind, affil in enumerate(affils):
                    affil_dict = dict()
                    full_name = affil.text
                    
                    try:
                        afid = affil.find_element(By.CSS_SELECTOR, 'a').get_attribute("href")
                        affil_dict['afid'] = afid[afid.find("afid=") + 5 : len(afid)]
                        
                        affil_name = affil.find_element(By.CSS_SELECTOR, 'a > span').text
                    except Exception:
                        affil_dict['afid'] = None
                        
                    if full_name.find(',') != -1:
                        name = full_name.split(',')[0]
                        affil_name = full_name
                        affil_county = full_name[full_name.rfind(',') + 2:]
                        affil_city = full_name[len(name) + 2: full_name.rfind(',')]  
                    else:
                        affil_name = full_name
                        affil_county = None
                        affil_city = None

                    affil_dict['affil_name'] = affil_name
                    affil_dict['affil_country'] = affil_county
                    affil_dict['affil_city'] = affil_city  

                    print(f"afid {affil_dict['afid']}")
                    print(f"affil_name {affil_dict['affil_name']}")
                    print(f"affil_country {affil_dict['affil_country']}")
                    print(f"affil_city {affil_dict['affil_city']}")
                
                    author['affils'].append(affil_dict)
                
                print(f"name {author['name']}")
                print(f"auid {author['auid']}")
                print(f"\n")
            except Exception as e:
                print(e) 
                continue

            authors.append(author)
            if len(contents) != author_li_len:
                count = 0
                while self.driver.find_elements(By.CSS_SELECTOR, "els-stack.stack--s") != author_li_len:
                    time.sleep(0.001)
                    count += 1
                    if count == 4000:
                        return list() 
                    
                contents = self.driver.find_elements(By.CSS_SELECTOR, "els-stack.stack--s")
                    
        print(f"len2 {len(authors)}")
        if author_li_len != len(authors):
           return list() 
        return authors
        

    def select_cited_by_search(self, url, item_count):
        try:
            print(f"cited_by\n") 
            self.driver.get(url)
            self.short_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'ul.pagination')))                   
            page_count = 0
            page = 0
            
            citations = []
            
            if item_count is None:
                print("get item_count")
                try:
                    item_count = self.short_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, '.headerBackground span'))).text.split()[0]
                except Exception as e:
                    return list() 

            if len(self.driver.find_elements(By.CSS_SELECTOR, 'ul.pagination > li')) > 1:
                self.short_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 
                    'div.row.paginationCont span.ui-selectmenu-icon.ui-icon.btn-primary.btn-icon.ico-navigate-down.flexDisplay.flexAlignCenter.flexJustifyCenter.flexColumn'))).click()
                self.short_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'li.ui-menu-item > div#ui-id-4'))).click()
               
                self.short_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'h1.documentHeader')))
                page_count = item_count // 200

           # print(f"page_count {page_count}\n")
            while page <= page_count:
                self.short_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'div#resultsPanel')))
                links = self.driver.find_elements(By.CSS_SELECTOR, 'tr.searchArea')
                links_len = len(links)
                print(f"page {page}\n")
                print(f"links {len(links)}\n")
                link_count = 0
                print(f"link_count {len(links)}\n")
                
                while link_count < links_len:
                    citation = dict()
                    
                    try:
                        body = links[link_count].find_element(By.CSS_SELECTOR, 'a.ddmDocTitle')
                        scopus_link = body.get_attribute("href")
                            
                        citation['eid'] = scopus_link[scopus_link.find("eid=") + 4 : scopus_link.find("&", scopus_link.find("eid="), len(scopus_link))]
                        citation['title'] = body.text  
                        citation['scopus_link'] = scopus_link
                        citation['cited_by_link'] = links[link_count].find_element(By.CSS_SELECTOR, 'td.textRight > a').get_attribute("href")
                        citation['is_scopus'] = True
                        
                    except NoSuchElementException as e:
                        cited_by_link = links[link_count].find_element(By.CSS_SELECTOR, 'td.textRight > a').get_attribute("href")
                        title = links[link_count].find_element(By.CSS_SELECTOR, 'span.txtOnlyDummy').text 
                            
                        citation['title'] = title
                        citation['cited_by_link'] = cited_by_link
                        
                        if cited_by_link.find("eid=") == -1:
                            citation['eid'] = cited_by_link[cited_by_link.find("eid=") + 4 : cited_by_link.find("&", cited_by_link.find("eid="), len(cited_by_link))]
                            citation['scopus_link'] = f"https://www.scopus.com/record/display.uri?eid={citation['eid']}&origin=reflist"
                            citation['is_scopus'] = True  
                        else:
                            citation['eid'] = cited_by_link
                            citation['scopus_link'] = cited_by_link
                            citation['is_scopus'] = False  
                    
                    #print(f"eid {citation['eid']}")
                    #print(f"title {citation['title']}")
                    #print(f"scopus_link {citation['scopus_link']}")
                    #print(f"cited_by_link {citation['cited_by_link']}")
                    #print(f"\n")
                                
                    citations.append(citation)
                    link_count += 1
                    
                page += 1
                    
                if page <= page_count:
                    current_url = self.driver.current_url
                    if current_url.find('&offset=') != -1:
                        self.driver.get(f"{current_url[:current_url.find('&offset=') + 8]}{page*200 + 1}{current_url[current_url.find('&offset=') + 8 + len(str(page*200)):]}")
                    else:
                        self.driver.get(f"{current_url}&offset={page*200 + 1}")
            
            return citations
            
        except Exception as e:
            print(e) 
            return list()
        
        
    def process_item(self, item):
        log_date = item['log_date']
        self.cur.execute(f"""UPDATE publication_{log_date} SET keywords = %(keywords)s, abstract = %(abstract)s WHERE eid = %(eid)s;""",
            {'keywords': item['keywords'], 'abstract': item['abstract'], 'eid': item['eid']})

        self.cur.execute(f"""select author.name, author.id, affiliation_id
            from publication_{log_date} JOIN publication_author_{log_date} 
            on publication_{log_date}.id=publication_id JOIN author on author.id=author_id
            where publication_{log_date}.id=%(id)s;""", {"id": item['pub_id']})
        affiliation_author_name = self.cur.fetchall()

        if len(affiliation_author_name) >= 1:
            self.cur.execute("""DELETE from author where id = %(id)s;""", {"id":affiliation_author_name[0][1]})
                    
            for affil in affiliation_author_name:
                self.cur.execute("""DELETE from affiliation where id = %(id)s;""", {"id":affil[2]})
      
        for author in item['authors']:
            self.cur.execute("""insert into author (auid, name) values (%(auid)s,%(name)s) returning id;""", {"auid": author["auid"], "name":author["name"]})
            author_id = self.cur.fetchone()[0]

            for affil in author['affils']:
                self.cur.execute("""INSERT INTO affiliation(afid, name, city, country)
                            values (%(afid)s, %(affil_name)s, %(affil_city)s, %(affil_country)s)
                            RETURNING id;""", {"afid": affil["afid"], "affil_name":affil["affil_name"], "affil_city":affil["affil_city"], "affil_country":affil["affil_country"]})

                affiliation_id = self.cur.fetchone()
                self.cur.execute(f"""insert into publication_author_{log_date} (log_date, publication_id, author_id, affiliation_id) values
                    (%(log_date)s, %(publication_id)s, %(author_id)s, %(affiliation_id)s) ON CONFLICT (log_date, publication_id, author_id, affiliation_id) DO NOTHING;""", 
                    {'log_date': log_date, 'publication_id': item['pub_id'], 'author_id': author_id, 'affiliation_id': affiliation_id[0]})

        for citation in item['citations']:
            self.cur.execute("""WITH cte AS (
                       INSERT into publication (eid, title, link_scopus, link_citedby, is_scopus)
                       values (%(eid)s, %(title)s, %(scopus_link)s, %(cited_by_link)s, %(is_scopus)s) 
                       ON CONFLICT (eid) DO NOTHING RETURNING id
                    )
                    SELECT id from cte;""", citation)

            publication_id = self.cur.fetchone()
            
            if publication_id is None:
                self.cur.execute("""SELECT id FROM publication WHERE eid = %(eid)s FOR SHARE;""" , {"eid": citation['eid']})
                publication_id = self.cur.fetchone()

            self.cur.execute("""insert into publication_citations (publication_id, publication_citation_id) values
            (%(publication_id)s, %(publication_citation_id)s) ON CONFLICT (publication_id, publication_citation_id) DO NOTHING;""", 
            {'publication_id': item['pub_id'], 'publication_citation_id': publication_id[0]})
            
        self.connection.commit()
        return item
        
a = A()
a.driver.quit()
a.connection.commit()
a.cur.close()
a.connection.close()