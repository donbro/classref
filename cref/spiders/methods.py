#
# encoding: utf-8

from cref.items import Method

from scrapy.spider import BaseSpider

from scrapy.selector import HtmlXPathSelector


# To create a Spider you must subclass scrapy.spider.BaseSpider and define three attributes

class MethodsSpider(BaseSpider): # Mass_methods(object):

    # = hxs.select('//div[@class="api classMethod"]')        class_methods = hxs.select('//div[@class="api classMethod"]')thod

    name = 'Method'
    # allowed_domains = ['mininova.org']

    #start_urls = ['file:///Users/donb/NSApplication%20Class%20Reference.html']

    start_urls = [ 'https://developer.apple.com/library/mac/documentation/cocoa/reference/ApplicationKit/Classes/NSApplication_Class/Reference/Reference.html' ]

    #rules = [Rule(SgmlLinkExtractor(allow=['/tor/\d+']), 'parse_torrent')]

    def parse(self, response):

        hxs = HtmlXPathSelector(response)

        #sites = hxs.select('//ul[@class="directory-url"]/li')

        items = []

        class_methods = hxs.select('//div[@class="api classMethod"]') 
        class_title = hxs.select('//h1/text()') 
        print "class_title is", class_title

        for class_method in class_methods:
            #item = Website()
            method = Method()

            method['type'] = 'classMethod'
            method['class_name'] = class_title.extract()
            method['method_name'] = class_method.select('h3[@class="jump classMethod"]/text()').extract()
            method['abstract'] = class_method.select('p[@class="abstract"]/text()').extract() 
            method['seeAlso'] = class_method.select('div[@class="api seeAlso"]/ul/li//a/text()').extract()

            #item['name'] = class_method.select('a/text()').extract()
            #item['url'] = class_method.select('a/@href').extract()
            #item['description'] = class_method.select('text()').re('-\s([^\n]*?)\\n')
            items.append(method)

        instance_methods = hxs.select('//div[@class="api instanceMethod"]') 

        for instance_method in instance_methods:
            #item = Website()
            method = Method()

            method['type'] = 'instanceMethod'
            method['class_name'] = class_title.extract()
            # method['method_name'] = class_method.select('h3[@class="jump classMethod"]/text()').extract()
            method['method_name'] = instance_method.select('h3[@class="jump instanceMethod"]/text()').extract()
            method['abstract'] = instance_method.select('p[@class="abstract"]/text()').extract() 
            method['seeAlso'] = instance_method.select('div[@class="api seeAlso"]/ul/li//a/text()').extract()

            #item['name'] = instance_method.select('a/text()').extract()
            #item['url'] = instance_method.select('a/@href').extract()
            
            items.append(method)

        return items

        #method['url'] = response.url
        #method['class_method'] = x.select('//div[@class="api classMethod"]').extract()
        #method['name'] = x.select('//h3[@class="jump classMethod"]').extract()
        #method['abstract'] = x.select('//p[@class="abstract"]').extract()
        #method['description'] = x.select("//div[@id='description']").extract()
        #method['size'] = x.select("//div[@id='info-left']/p[2]/text()[2]").extract()

        #return method

##  name: identifies the Spider. It must be unique, that is, you can’t set the same name for different Spiders.
##  start_urls: is a list of URLs where the Spider will begin to crawl from. 
##      So, the first pages downloaded will be those listed here. 
##      The subsequent URLs will be generated successively from data contained in the start URLs.
##  parse() is a method of the spider, which will be called with the 
##      downloaded Response object of each start URL. 
##      The response is passed to the method as the first and only argument.
# This method is responsible for parsing the response data and 
##      extracting scraped data (as scraped items) and more URLs to follow.
# The parse() method is in charge of processing the response and 
##      returning scraped data (as Item objects) and more URLs to follow (as Request objects).
