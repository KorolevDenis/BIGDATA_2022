# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

# pipelines.py
import psycopg2
        
class WebScopusCrawlerPipeline:
    def __init__(self):
        self.hostname = os.getenv('POSTGRES_HOST')
        self.username = os.getenv('POSTGRES_USER')
        self.password = os.getenv('POSTGRES_PASSWORD')
        self.database = os.getenv('POSTGRES_DB')

        self.connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database)
        self.connection.set_client_encoding('UTF8')
        self.cur = self.connection.cursor()
    
    
    def process_item(self, item, spider):
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


    def close_spider(self, spider):
        self.connection.commit()
        self.cur.close()
        self.connection.close()