import scrapy
import time
from scrapy import Request
from selenium import webdriver
from webdriver_manager.firefox import GeckoDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from selenium.webdriver import DesiredCapabilities
import time
from selenium import webdriver
from fake_useragent import UserAgent
from selenium.webdriver.common.proxy import Proxy, ProxyType
import psycopg2
import sys
import string
import json

sys.path.append('/home/BIGDATA-SCOPUS/scopusCrawler/WebScopusCrawler/WebScopusCrawler')

WAIT_TIME = 30

chrome_cookies = ''
#COOKIES = dict([l.split("=", 1) for l in chrome_cookies.replace(" ", "").split(";")])
USERNAME = ""
PASSWORD = ""
INSTITUTION = 'Санкт-Петербургский политехнический университет Петра Великого'

class WebScopusSpider(scrapy.Spider):
    name = 'web_spider'

    def __init__(self, domains=None, name=None, *args, **kwargs):
        kwargs.pop('_job')
        super(WebScopusSpider, self).__init__(*args, **kwargs)
        self.subjects = json.loads(domains)

        try:
            self.logger.info("sadad")
            #USERNAME = args[0]
            #PASSWORD = args[1]

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
            caps = DesiredCapabilities().FIREFOX
            caps["pageLoadStrategy"] = "eager"  #  interactive
            caps["marionette"] = "false"
            options.add_argument(f'user-agent="{user_agent}"')
            #options.add_experimental_option('excludeSwitches', ['enable-logging'])
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            #options.add_argument('--proxy-server=%s' % myProxy)
            options.add_argument('--window-size=1420,1080')
            self.driver = webdriver.Remote(command_executor="http://localhost:4444/wd/hub",desired_capabilities=DesiredCapabilities.FIREFOX)
            self.wait = WebDriverWait(self.driver, WAIT_TIME)
            self.short_wait = WebDriverWait(self.driver, 5)
            self.shortest_wait = WebDriverWait(self.driver, 2)
            self.alphabet_dict = dict((key, ind) for ind, key in enumerate(string.ascii_lowercase))

            self.proc_count = int(self.subjects[0])
            self.proc_number = int(self.subjects[1])
            self.left_date = int(self.subjects[2])
            self.right_date = int(self.subjects[3])
            USERNAME = self.subjects[4]
            PASSWORD = self.subjects[5]
            self.hostname = 'localhost'
            self.username = 'denis'
            self.password = ''
            self.database = 'denis'

            self.connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database)
            self.connection.set_client_encoding('UTF8')
            self.cur = self.connection.cursor()
            self.log_in("pass")
            print("after")
        except Exception as e:
            self.logger.info(e)


    def start_requests(self):
        self.logger.info("adawdawd")
        date = self.right_date - self.proc_number
        self.proc_count = 1
        self.logger.info("in requests")
        try:
            req_count = 0
            offset_count = 0
            rec_count = 100
            request_count = 0

            self.cur.execute(f"""SELECT link_scopus, link_citedby, eid, publication_{date}.id FROM publication_{date} JOIN publication_author_{date} on
                            publication_{date}.id = publication_id JOIN affiliation on affiliation_id = affiliation.id WHERE city LIKE '%Saint Petersburg%' or city LIKE '%Leningrad%'
                            ORDER BY id LIMIT {rec_count};""")
            publications = self.cur.fetchall()

            self.logger.info(f"active_count() {active_count()}")
            active_c = active_count()
            while offset_count * rec_count + req_count < count:
                yield Request("https://www.google.com/",
                   callback = self.parse, meta = {"link_scopus": publications[req_count % rec_count][0], "link_citedby": publications[req_count % rec_count][1],
                   "eid": publications[req_count % rec_count][2], "id": publications[req_count % rec_count][3], "log_date":date}, dont_filter=True)

                req_count += 1

                if req_count % rec_count == 0:
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
                        self.logger.info(f"date {date}")

        except Exception as e:
            self.logger.info(e)

    def log_in(self, type):
        try:
            if (type == "cockies"):
                self.logger.info('Logging in via cookies ...')

                self.driver.get('https://www.scopus.com/search/form.uri?display=advanced')

                # Add cookies to the website of Scopus:
                self.logger.info('-- Adding cookies ...')
                self.logger.info(COOKIES)
                for name, value in COOKIES.items():
                    self.driver.add_cookie({
                        'name': name,
                        'value': value})

                self.logger.info('-- Testing whether you successfully logged in ...')

                self.driver.get('https://www.scopus.com/search/form.uri?display=advanced')
                time.sleep(
                    SLEEPTIME_LONG)  # Please lengthen this waiting time, when your network can successfully log in manually but not via this program.
                judge = self.driver.current_url.count("advanced")
                self.logger.info(judge)
                if judge > 0:
                    self.logger.info('-- Successfully logged in!')
                if judge == 0:
                    self.logger.info('-- Failed to log in.')
                    self.log_in(type)
            elif (type == "pass"):
                self.driver.get(
                    'https://id.elsevier.com/as/authorization.oauth2?platSite=SC%2Fscopus&ui_locales=en_US&scope=openid+profile+email+els_auth_info+els_analytics_info+urn%3Acom%3Aelsevier%3Aidp%3Apolicy%3Aproduct%3Ainst_assoc&response_type=code&redirect_uri=https%3A%2F%2Fwww.scopus.com%2Fauthredirect.uri%3FtxGid%3Dbc6f0c0ea70010df705a26ab6916511f&state=checkAccessLogin&authType=SINGLE_SIGN_IN&prompt=login&client_id=SCOPUS')
                self.logger.info('-- Inputting, submitting, and choosing your institution ...')
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
                self.logger.info(
                    '-- Redicrecting to the VPN of your institution. Inputting and submitting your username and password ...')
                try:
                    username_institution = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#user')))
                    password_institution = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#password')))
                    username_institution.send_keys(USERNAME)
                    password_institution.send_keys(PASSWORD)
                    submit_institution = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '#doLogin')))
                    submit_institution.click()
                except TimeoutException:
                    self.logger.info('scip inst')

                # Test whether you successfully logged in:
                self.logger.info('-- Testing whether you successfully logged in ...')
                self.driver.get('https://www.scopus.com/search/form.uri?display=advanced')
                time.sleep(
                    20)  # Please lengthen this waiting time, when your network can successfully log in manually but not via this program.
                judge = self.driver.current_url.count("advanced")
                self.logger.info(judge)
                if judge > 0:
                    self.logger.info('-- Successfully logged in!')
                    return
                if judge == 0:
                    self.logger.info('-- Failed to log in.')
                    self.log_in(type)
        except Exception as e:
            self.logger.info('Try again ...')
            self.logger.info(e)
            self.log_in(type)


    def get_request_count(self):
        self.cur.execute("""SELECT count(id) FROM publication WHERE is_scopus = TRUE;""")
        return self.cur.fetchone()[0]


    def get_request_count(self, date):
        self.cur.execute(f"""SELECT count(publication_{date}.id) FROM publication_{date} JOIN publication_author_{date} on
                                publication_{date}.id = publication_id JOIN affiliation on affiliation_id = affiliation.id WHERE city LIKE '%Saint Petersburg%' or city LIKE '%Leningrad%';""")
        return self.cur.fetchone()[0]


    def parse(self, response):
        try:
            self.driver.get(response.meta["link_scopus"])
            self.logger.info(response.meta["link_scopus"])
            result['eid'] = response.meta["eid"]
            result['pub_id'] = response.meta["id"]
            url_cited_by = response.meta["link_citedby"]
            result['abstract'] = None
            result['keywords'] = None

            try:
                result['abstract'] = self.short_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'section.col-24.col-lg-18 p > span'))).text
            except Exception as e:
                self.logger.info("result['abstract']")

            try:
                keywords = []
                self.short_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'section.col-24.col-lg-18 .margin-size-4-t > span')))
                for keyword in self.driver.find_elements(By.CSS_SELECTOR, 'section.col-24.col-lg-18 .margin-size-4-t > span'):
                    keywords.append(keyword.text)

                result['keywords'] = '; '.join(keywords)
            except Exception as e:
                self.logger.info("result['keywords']")

            result['authors'] = self.select_authors()
            result['log_date'] = response.meta["log_date"]
            result['citations'] = list()

            self.logger.info(f"link_count {len(result['citations'])}\n")

            yield result
        except Exception as e:
            self.logger.info(e)


    def select_authors_with_id_and_cit(self, url_cited_by):
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
            authors = self.select_authors_with_id()
            citations = self.select_cited_by_search(url_cited_by, count)

            return authors, citations

        self.logger.info(f"count {count}\n")
        if count > 80:
            try:
                href = self.driver.find_element(By.CSS_SELECTOR, 'ul.refTitleCount.list-inline > li.pull-right > a').get_attribute("href")
            except Exception:
                href = url_cited_by
            authors = self.select_authors_with_id()
            citations = self.select_cited_by_search(href, count)
        else:
            link_count = 0
            self.short_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'tbody.referencesUL')))
            links = self.driver.find_elements(By.CSS_SELECTOR, 'tbody.referencesUL > tr > td.refCont')

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

                citations.append(citation)
                link_count += 1
            authors = self.select_authors_with_id()

        return authors, citations


    def select_authors_with_id(self):
        authors = []

        try:
            author_li = self.shortest_wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR,
                'div.row > div.col-24.col-lg-18.col-xl-16 > ul.ul--horizontal.margin-size-0 > li > els-button')))
        except Exception as e:
            return list()

        author_li_len = len(author_li)
        if author_li_len == 0:
            return list()

        self.logger.info(f"len {author_li_len}")
        for author_index in range(0, author_li_len):
            self.driver.execute_script(
                f"document.querySelectorAll('div.row > div.col-24.col-lg-18.col-xl-16 > ul.ul--horizontal.margin-size-0 > li > els-button')[{author_index}].shadowRoot.querySelector('button').click()")

        try:
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'els-stack.stack--s')))
        except Exception as e:
            return list()

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

                    author['affils'].append(affil_dict)

            except Exception as e:
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

        if author_li_len != len(authors):
           return list()
        return authors


    def select_authors(self):
        authors = []

        try:
            self.short_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR,
                'div.row > div.col-24.col-lg-18.col-xl-16 > ul.ul--horizontal.margin-size-0 > li > els-button')))
        except Exception as e:
            return authors

        for i in range(0, len(self.driver.find_elements(By.CSS_SELECTOR, 'section.col-24.col-xl-18.padding-size-0-b span.button__text'))):
            self.driver.execute_script(f"document.querySelectorAll('section.col-24.col-xl-18.padding-size-0-b span.button__text')[{i}].click()")

        try:
            author_list = self.short_wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR,
                'div.row > div.col-24.col-lg-18.col-xl-16 > ul.ul--horizontal.margin-size-0 > li')))
        except Exception as e:
            return list()

        try:
            affils = self.short_wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '#affiliation-section li > span')))
        except Exception as e:
            affils = []

        author_li_len = len(author_list)

        if len(affils) == 1:
            for author_li in author_list:
                author = dict()
                try:
                    author['name'] = author_li.find_element(By.CSS_SELECTOR, 'els-button').text
                    author['auid'] = None
                    author['affils'] = []

                    affil_dict = dict()

                    full_name = affils[0].text
                    affil_dict['afid'] = None

                    if full_name.rfind(',') != -1:
                        name = full_name.split(',')[0]
                        affil_dict['affil_full_name'] = full_name
                        affil_dict['affil_country'] = full_name[full_name.rfind(',') + 2: len(full_name)]
                        affil_dict['affil_city'] = full_name[len(name) + 2: full_name.rfind(',')]
                    else:
                        affil_dict['affil_full_name'] = full_name
                        affil_dict['affil_country'] = None
                        affil_dict['affil_city'] = None

                except Exception as e:
                    continue

                authors.append(author)
        elif len(affils) != 0:
            affil_list = []
            for affil in affils:
                affil_list.append(affil.text)

            for author_li in author_list:
                author = dict()
                try:
                    author['name'] = author_li.find_element(By.CSS_SELECTOR, 'els-button').text
                    author['auid'] = None
                    author['affils'] = []

                    affil_label = author_li.find_element(By.CSS_SELECTOR, 'span').text.split(', ')
                    for ind, affil in enumerate(affil_label):
                        affil_dict = dict()

                        affil_dict['afid'] = None
                        full_name = affil_list[self.alphabet_dict[affil]]

                        if full_name.rfind(',') != -1:
                            name = full_name.split(',')[0]
                            affil_dict['affil_full_name'] = full_name
                            affil_dict['affil_country'] = full_name[full_name.rfind(',') + 2: len(full_name)]
                            affil_dict['affil_city'] = full_name[len(name) + 2: full_name.rfind(',')]
                        else:
                            affil_dict['affil_full_name'] = full_name
                            affil_dict['affil_country'] = None
                            affil_dict['affil_city'] = None

                        author['affils'].append(affil_dict)

                except Exception as e:
                    continue

                authors.append(author)
        else:
            for author_li in author_list:
                author = dict()
                try:
                    author['name'] = author_li.find_element(By.CSS_SELECTOR, 'els-button').text
                    author['auid'] = None
                    author['affils'] = []
                except Exception as e:
                    continue

                authors.append(author)

        if author_li_len != len(authors):
           return list()
        return authors


    def select_cited_by_search(self, url, item_count):
        try:
            self.driver.get(url)
            self.short_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'ul.pagination')))                 
            page_count = 0
            page = 0

            citations = []

            if item_count is None:
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

           # self.logger.info(f"page_count {page_count}\n")
            while page <= page_count:
                self.short_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'div#resultsPanel')))
                links = self.driver.find_elements(By.CSS_SELECTOR, 'tr.searchArea')
                links_len = len(links)
                link_count = 0

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
            self.logger.info(e)
            return list()


    def spider_closed(self, spider):
        self.cur.close()
        self.connection.close()
        self.driver.quit()