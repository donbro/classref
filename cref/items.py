# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/topics/items.html

from scrapy.item import Item, Field

class Method(Item):
    url = Field()
    type = Field()
    method_name = Field()
    class_method = Field()
    class_name = Field()
    abstract = Field()
    seeAlso = Field()
    size = Field()

#class DmozItem(Item):
#    # define the fields for your item here like:
#    title = Field()
#    link = Field()
#    desc = Field()

# [http://doc.scrapy.org/en/latest/intro/tutorial.html]
