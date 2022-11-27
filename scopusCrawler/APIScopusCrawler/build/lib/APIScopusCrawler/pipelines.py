# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

# pipelines.py
import psycopg2
import subprocess

class APIScopusCrawlerPipeline:
    def __init__(self):
        self.hostname = 'localhost'
        self.username = 'denis'
        self.password = 'qw'
        self.database = 'denis'

        self.connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database)
        self.connection.set_client_encoding('UTF8')
        self.cur = self.connection.cursor()
        
        self.freetoread = {
            'all': 1,
            'publisherhybridgold': 2,
            'repository': 3,
            'repositoryvor': 4,
            'repositoryam': 5,
            'publisherfree2read': 6
        }
        
        self.freetoread_label = {
            'All Open Access': 1,
            'Green': 2,
            'Hybrid Gold': 3
        }
        
        self.count = 0
        self.affil_count = 0
        self.flag = False
        self.publication_name = ""
        
    def dump_db(self):
        fh = open("NUL","w")
        process = subprocess.Popen(
            ['pg_dump',
             '--dbname=postgresql://{}:{}@{}:{}/{}'.format(self.username, self.password, self.hostname, 5432, self.database),
             '-Fc',
             '-f', 'backup.sql',
             '-v'],
            stdout=fh, 
            stderr=subprocess.PIPE
        )
        fh.close()
        
        process.communicate()


    def process_item(self, item, spider):
        try:
            if not self.flag:
                self.publication_name = f"publication_{item['publication']['log_date']}"
                self.publication_author_name = f"publication_author_{item['publication']['log_date']}"
                self.publication_subject_name = f"publication_subject_{item['publication']['log_date']}"
                self.publication_freetoread_name = f"publication_freetoread_{item['publication']['log_date']}"
                self.publication_freetoread_label_name = f"publication_freetoread_label_{item['publication']['log_date']}"
                
                #self.cur.execute(f"DROP TABLE IF EXISTS {self.publication_name} CASCADE;")
                #self.cur.execute(f"DROP TABLE IF EXISTS {self.publication_author_name} CASCADE;")
                #self.cur.execute(f"DROP TABLE IF EXISTS {self.publication_subject_name} CASCADE;")
                #self.cur.execute(f"DROP TABLE IF EXISTS {self.publication_freetoread_name} CASCADE;")
                #self.cur.execute(f"DROP TABLE IF EXISTS {self.publication_freetoread_label_name} CASCADE;")
                
                self.cur.execute(f"""CREATE TABLE {self.publication_name} (
                                    LIKE publication INCLUDING DEFAULTS INCLUDING CONSTRAINTS
                                    );

                                    ALTER TABLE publication ATTACH PARTITION {self.publication_name} FOR VALUES IN ({item['publication']['log_date']});
                                    create unique index {self.publication_name}_ind on {self.publication_name} (dc_identifier);""")

                self.cur.execute(f"""CREATE TABLE {self.publication_author_name} (
                                    LIKE publication_author INCLUDING DEFAULTS INCLUDING CONSTRAINTS
                                    );
                                    ALTER TABLE publication_author ATTACH PARTITION {self.publication_author_name} FOR VALUES IN ({item['publication']['log_date']});
                                    
                                    CREATE TABLE {self.publication_subject_name} (
                                    LIKE publication_subject INCLUDING DEFAULTS INCLUDING CONSTRAINTS
                                    );
                                    ALTER TABLE publication_subject ATTACH PARTITION {self.publication_subject_name} FOR VALUES IN ({item['publication']['log_date']});
                                                                    
                                    CREATE TABLE {self.publication_freetoread_name} (
                                    LIKE publication_freetoread INCLUDING DEFAULTS INCLUDING CONSTRAINTS
                                    );
                                    ALTER TABLE publication_freetoread ATTACH PARTITION {self.publication_freetoread_name} FOR VALUES IN ({item['publication']['log_date']});
                                                                    
                                    CREATE TABLE {self.publication_freetoread_label_name} (
                                    LIKE publication_freetoread_label INCLUDING DEFAULTS INCLUDING CONSTRAINTS
                                    );
                                    ALTER TABLE publication_freetoread_label ATTACH PARTITION {self.publication_freetoread_label_name} FOR VALUES IN ({item['publication']['log_date']});""")

                self.flag = True
                
            self.count += 1
            
            self.cur.execute("""WITH cte AS (
                           INSERT INTO journal(source_id, name, issn, e_issn, aggregation_type, cite_score, sjr, snip, year_of_scores, publisher)
                           values (null,%(name)s,%(issn)s,%(e_issn)s,%(aggregation_type)s, null, null, null, null, null)
                           ON CONFLICT (issn, e_issn, name) DO NOTHING
                           RETURNING id
                        )
                        SELECT id from cte;""" , item["journal"])
                        
            journal_id = self.cur.fetchone()
            if journal_id is None:
                self.cur.execute("""SELECT id FROM journal
                    WHERE issn = %(issn)s and e_issn = %(e_issn)s and name = %(name)s FOR SHARE;""" , item["journal"])
                        
                journal_id = self.cur.fetchone()       
                            
            item["publication"]['journal_id'] = journal_id

            self.cur.execute(f"""WITH cte AS (
                           INSERT into {self.publication_name} (link_scopus, link_citedby, dc_identifier, eid, title, volume, issue_identifier, page_range,
                           cover_date, doi, citedby_count, pubmed_id, journal_id, subtype, article_number, source_id, openaccess, keywords, abstract, raw_xml, cur_time, api_key, is_scopus, log_date)
                           values (%(link_scopus)s,%(link_citedby)s, %(scopus_id)s,%(eid)s,%(title)s, %(volume)s,%(issue_identifier)s,%(page_range)s,
                           %(cover_date)s,%(doi)s,%(citedby_count)s, %(pubmed_id)s,%(journal_id)s,%(subtype)s, %(article_number)s,%(source_id)s,%(openaccess)s, null, null,%(raw_xml)s, %(cur_time)s, 
                           %(api_key)s, TRUE, %(log_date)s) 
                           ON CONFLICT (dc_identifier) DO NOTHING RETURNING id
                        )
                        SELECT true, id, -1 from cte;""", item["publication"])
            
            publication_id = self.cur.fetchone()
            if publication_id is None:
                self.cur.execute(f"""SELECT false, id, dc_identifier FROM {self.publication_name}
                        WHERE dc_identifier = %(scopus_id)s;""", item["publication"])
                        
                publication_id = self.cur.fetchone()
            
            spider.logger.info(f"({publication_id[1]}, {publication_id[2]}) {publication_id[0]} {item['publication']['log_date']} {item['publication']['subject_id']} {item['url']}")
        
            try:
                self.cur.execute(f"""insert into {self.publication_subject_name} (log_date, publication_id, subject_id) values
                    (%(log_date)s, %(publication_id)s, %(subject_id)s) ON CONFLICT (log_date, publication_id, subject_id) DO NOTHING;""", 
                    {'log_date': item['publication']['log_date'], 'publication_id': publication_id[1], 'subject_id': item['publication']['subject_id']})
            except Exception as e:
                spider.logger.info(e)
            
            if publication_id[0]:
                afid_ids = []
                if item["author"] != "":
                    self.cur.execute("""insert into author (auid, name) values (null, %(author)s) returning id;""", {'author': item["author"]})
                    author_id = self.cur.fetchone()

                    for ind, affiliation in enumerate(item["affiliations"]):

                        self.cur.execute("""INSERT INTO affiliation(afid, name, city, country) values (null, %(name)s, %(city)s, %(country)s) RETURNING id;""", affiliation)
                        
                        afid_ids += self.cur.fetchone()
                
                for afid_id in afid_ids:
                    self.cur.execute(f"""insert into {self.publication_author_name} (log_date, publication_id, author_id, affiliation_id) values
                        (%(log_date)s, %(publication_id)s, %(author_id)s, %(affiliation_id)s);""", 
                        {'log_date': item['publication']['log_date'], 'publication_id': publication_id[1], 'author_id': author_id, 'affiliation_id': afid_id})
                        
                self.insert_freetoread(item, publication_id[1])
                
            elif publication_id[2] is None:
                self.cur.execute(f"""UPDATE {self.publication_name} SET link_scopus =%(link_scopus)s, link_citedby =%(link_citedby)s, dc_identifier = %(scopus_id)s, 
                           eid = %(eid)s, title = %(title)s, volume = %(volume)s, issue_identifier = %(issue_identifier)s, page_range = %(page_range)s,
                           cover_date = %(cover_date)s, doi = %(doi)s, citedby_count = %(citedby_count)s, pubmed_id = %(pubmed_id)s,
                           subtype = %(subtype)s, article_number = %(article_number)s, source_id = %(source_id)s, openaccess = %(openaccess)s,
                           raw_xml = %(raw_xml)s, cur_time = %(cur_time)s, api_key = %(api_key)s, log_date = %(log_date)s WHERE dc_identifier = %(scopus_id)s;""", item["publication"])
                
                self.insert_freetoread(item, publication_id[1])
        
            if self.count % 10 == 0:
                self.connection.commit()

        except Exception as e:
            print(e)
            self.connection.rollback()
            
        return item

    def insert_freetoread(self, item, publication_id):
        if item["publication"]["openaccess"]:
            for freetoread in item["freetoread"]:
                if freetoread not in self.freetoread:
                    self.cur.execute("""insert into freetoread (name) values
                        (%(name)s) returning id;""", {'name': freetoread})
                    self.freetoread[freetoread] = self.cur.fetchone()
                
                self.cur.execute(f"""insert into {self.publication_freetoread_name} (log_date, publication_id, freetoread_id) values
                (%(log_date)s, %(publication_id)s, %(freetoread_id)s);""", 
                {'log_date': item['publication']['log_date'], 'publication_id': publication_id, 'freetoread_id': self.freetoread[freetoread]})
                
                
            for freetoread_label in item["freetoread_label"]:
                if freetoread_label not in self.freetoread_label:
                    self.cur.execute("""insert into freetoread_label (name) values
                        (%(name)s) returning id;""", {'name': freetoread_label})
                    self.freetoread_label[freetoread_label] = self.cur.fetchone()
                
                self.cur.execute(f"""insert into {self.publication_freetoread_label_name} (log_date, publication_id, freetoread_label_id) values
                (%(log_date)s, %(publication_id)s, %(freetoread_label_id)s);""",
                {'log_date': item['publication']['log_date'], 'publication_id': publication_id, 'freetoread_label_id': self.freetoread_label[freetoread_label]})


    def close_spider(self, spider):
        self.connection.commit()
        self.dump_db()
        self.cur.close()
        self.connection.close()
        