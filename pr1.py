import scrapy
import random
import time
from scrapy.crawler import CrawlerProcess

class CoinMarketCapSpider(scrapy.Spider):
    name = "coinmarketcap"
    allowed_domains = ["coinmarketcap.com"]
    start_urls = ["https://coinmarketcap.com/"]
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOAD_DELAY': random.uniform(1, 3),  #Evita saturar el servidor
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 5,
        'FEEDS': {
            'coin_data.json': {
                'format': 'json',
                'encoding': 'utf8',
                'indent': 4,
            },
        },
    }

    def parse(self, response):
        """ Extrae enlaces a las páginas individuales de cada criptomoneda. """
        crypto_links = response.css("tr td:nth-child(3) a::attr(href)").getall()
        for link in crypto_links:
            absolute_url = response.urljoin(link)
            yield scrapy.Request(url=absolute_url, callback=self.parse_crypto)
            time.sleep(random.uniform(1, 2))  # Evita bloqueos por scraping agresivo

    def parse_crypto(self, response):
        """ Extrae información de cada criptomoneda. """
        yield {
            'name': response.css("h2::text").get(),
            'symbol': response.css("small::text").get(),
            'price': response.css("div.priceValue span::text").get(),
            'market_cap': response.css("div.statsValue::text").get(),
            'volume_24h': response.css("div span[data-target='volumeValue']::text").get(),
            'circulating_supply': response.css("div span[data-target='supplyValue']::text").get()
        }

process = CrawlerProcess()
process.crawl(CoinMarketCapSpider)
process.start()