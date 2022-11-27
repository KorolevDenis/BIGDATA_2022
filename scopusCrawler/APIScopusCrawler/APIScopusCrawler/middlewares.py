# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from logging import getLogger, Logger
from typing import Optional, Union

from twisted.internet import defer
from twisted.internet.error import (
    ConnectError,
    ConnectionDone,
    ConnectionLost,
    ConnectionRefusedError,
    DNSLookupError,
    TCPTimedOutError,
    TimeoutError,
)
from twisted.web.client import ResponseFailed

from scrapy.core.downloader.handlers.http11 import TunnelError
from scrapy.exceptions import NotConfigured
from scrapy.http.request import Request
from scrapy.spiders import Spider
from scrapy.utils.python import global_object_name
from scrapy.utils.response import response_status_message

class ScopuscrawlerSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class ScopuscrawlerDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


retry_logger = getLogger(__name__)

def get_retry_request(
    request: Request,
    *,
    spider: Spider,
    reason: Union[str, Exception] = 'unspecified',
    max_retry_times: Optional[int] = None,
    priority_adjust: Optional[int] = None,
    logger: Logger = retry_logger,
    stats_base_key: str = 'retry',
):
    settings = spider.crawler.settings
    stats = spider.crawler.stats
    retry_times = request.meta.get('retry_times', 0) + 1
    if max_retry_times is None:
        max_retry_times = request.meta.get('max_retry_times')
        if max_retry_times is None:
            max_retry_times = settings.getint('RETRY_TIMES')
    if retry_times <= max_retry_times:
        logger.debug(
            "Retrying %(request)s (failed %(retry_times)d times): %(reason)s",
            {'request': request, 'retry_times': retry_times, 'reason': reason},
            extra={'spider': spider}
        )
        new_request: Request = request.copy()
        new_request.meta['retry_times'] = retry_times
        new_request.dont_filter = True
        if priority_adjust is None:
            priority_adjust = settings.getint('RETRY_PRIORITY_ADJUST')
        new_request.priority = request.priority + priority_adjust

        if callable(reason):
            reason = reason()
        if isinstance(reason, Exception):
            reason = global_object_name(reason.__class__)

        stats.inc_value(f'{stats_base_key}/count')
        stats.inc_value(f'{stats_base_key}/reason_count/{reason}')
        return new_request
    else:
        stats.inc_value(f'{stats_base_key}/max_reached')
        logger.error(
            "Gave up retrying %(request)s (failed %(retry_times)d times): "
            "%(reason)s",
            {'request': request, 'retry_times': retry_times, 'reason': reason},
            extra={'spider': spider},
        )
        return None

class RetryMiddleware:

    # IOError is raised by the HttpCompression middleware when trying to
    # decompress an empty response
    EXCEPTIONS_TO_RETRY = (defer.TimeoutError, TimeoutError, DNSLookupError,
                           ConnectionRefusedError, ConnectionDone, ConnectError,
                           ConnectionLost, TCPTimedOutError, ResponseFailed,
                           IOError, TunnelError)

    def __init__(self, settings):
        if not settings.getbool('RETRY_ENABLED'):
            raise NotConfigured
        self.max_retry_times = settings.getint('RETRY_TIMES')
        self.retry_http_codes = set(int(x) for x in settings.getlist('RETRY_HTTP_CODES'))
        self.priority_adjust = settings.getint('RETRY_PRIORITY_ADJUST')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            url = request.url
            request.url = f"{url[:url.find('?apiKey=') + 8]}{spider.apiKey}{url[url.find('&subj='):]}"
            return self._retry(request, reason, spider) or response
        return response

    def process_exception(self, request, exception, spider):
        if (
            isinstance(exception, self.EXCEPTIONS_TO_RETRY)
            and not request.meta.get('dont_retry', False)
        ):
            url = request.url
            request.url = f"{url[:url.find('?apiKey=') + 8]}{spider.apiKey}{url[url.find('&subj='):]}"
            return self._retry(request, exception, spider)

    def _retry(self, request, reason, spider):
        max_retry_times = request.meta.get('max_retry_times', self.max_retry_times)
        priority_adjust = request.meta.get('priority_adjust', self.priority_adjust)
        return get_retry_request(
            request,
            reason=reason,
            spider=spider,
            max_retry_times=max_retry_times,
            priority_adjust=priority_adjust,
        )
