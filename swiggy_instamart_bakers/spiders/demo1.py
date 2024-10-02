import scrapy


class Demo1Spider(scrapy.Spider):
    name = "demo1"
    allowed_domains = ["google.com"]
    start_urls = ["https://google.com"]

    def parse(self, response):
        pass
