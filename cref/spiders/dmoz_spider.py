#
# encoding: utf-8


# To create a Spider, you must subclass scrapy.spider.BaseSpider, 
# and define the three main, mandatory, attributes:

##  name: identifies the Spider. It must be unique, that is, 
#
# you can’t set the same name for different Spiders.

##  start_urls: is a list of URLs where the Spider will begin to crawl from. 
# So, the first pages downloaded will be those listed here. 
# The subsequent URLs will be generated successively from data contained in the start URLs.

##  parse() is a method of the spider, which will be called with the downloaded 
# Response object of each start URL. The response is passed to the 
# method as the first and only argument.

# This method is responsible for parsing the response data and 
# extracting scraped data (as scraped items) and more URLs to follow.

# The parse() method is in charge of processing the response and 
# returning scraped data (as Item objects) and more URLs to follow (as Request objects).

# This is the code for our first Spider; save it in a file named dmoz_spider.py under the dmoz/spiders directory:

from scrapy.spider import BaseSpider

class DmozSpider(BaseSpider):
    name = "dmoz"
    allowed_domains = ["dmoz.org"]
    start_urls = [
        "http://www.dmoz.org/Computers/Programming/Languages/Python/Books/",
        "http://www.dmoz.org/Computers/Programming/Languages/Python/Resources/"
    ]

    def parse(self, response):
        filename = response.url.split("/")[-2]
        open(filename, 'wb').write(response.body)

