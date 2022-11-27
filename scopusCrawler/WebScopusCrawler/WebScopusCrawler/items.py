import scrapy

class WebResultItem(scrapy.Item):
    authors = scrapy.Field()
    citations = scrapy.Field()
    keywords = scrapy.Field()
    abstract = scrapy.Field()
    eid = scrapy.Field()
    pub_id = scrapy.Field()


