from web_spider import WebScopusSpider
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings     
from twisted.internet import reactor
from twisted.internet.task import deferLater

def sleep(self, *args, seconds):
   """Non blocking sleep callback"""
   return deferLater(reactor, seconds, lambda: None)

process = CrawlerProcess(get_project_settings())

process.crawl(WebScopusSpider)

process.start()