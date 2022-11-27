import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.crawler import CrawlerProcess

class MySpider(scrapy.Spider):
    # ... Your spider definition ...

# ... run it ...

process = CrawlerProcess(settings={ ... })    
process.crawl(MySpider)
process.start() # the script will block here until the crawling is finished