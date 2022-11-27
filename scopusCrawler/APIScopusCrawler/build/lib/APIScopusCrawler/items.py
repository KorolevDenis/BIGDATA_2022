import scrapy

class APIAffiliationItem(scrapy.Item):
    afid = scrapy.Field()
    name = scrapy.Field()
    city = scrapy.Field()
    country = scrapy.Field()
    
class APIAuthorItem(scrapy.Item):
    auid = scrapy.Field()
    name = scrapy.Field()
    country = scrapy.Field()

class APIJournalItem(scrapy.Item):
    scopus_id = scrapy.Field()
    name = scrapy.Field()
    issn = scrapy.Field()
    e_issn = scrapy.Field()
    aggregation_type = scrapy.Field()
    
    cite_score = scrapy.Field()
    sjr = scrapy.Field()
    snip = scrapy.Field()
    year_of_scores = scrapy.Field()

class APIArticleItem(scrapy.Item):
    link_scopus = scrapy.Field()
    link_citedby = scrapy.Field()
    
    scopus_id = scrapy.Field()
    eid = scrapy.Field()
    title = scrapy.Field()
    subject_id = scrapy.Field()
    journal_id = scrapy.Field()
    
    volume = scrapy.Field()
    issue_identifier = scrapy.Field()
    page_range = scrapy.Field()
    cover_date = scrapy.Field()
    doi = scrapy.Field()
    description = scrapy.Field()
    citedby_count = scrapy.Field()
    pubmed_id = scrapy.Field()
    aggregation_type = scrapy.Field()
    subtype = scrapy.Field()
    article_number = scrapy.Field()
    source_id = scrapy.Field()
    openaccess = scrapy.Field()
    
    freetoread = scrapy.Field()
    freetoread_label = scrapy.Field()
    
    raw_xml = scrapy.Field()
    cur_time = scrapy.Field()
    api_key = scrapy.Field()
    
class APIResultItem(scrapy.Item):
    publication = scrapy.Field()
    author = scrapy.Field()
    journal = scrapy.Field()
    freetoread = scrapy.Field()
    freetoread_label = scrapy.Field()
    affiliations = scrapy.Field()
