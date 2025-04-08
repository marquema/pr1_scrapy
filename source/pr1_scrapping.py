import scrapy
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor, task
from datetime import datetime
import os
import json
import csv
import json
import matplotlib.pyplot as plt
import json
import plotly.graph_objects as go
from datetime import datetime
import os

class CoinMarketCapSpider(scrapy.Spider):
    #atributos de la clase
    name = "coinmarketcap"
    allowed_domains = ["coinmarketcap.com"]
    start_urls = ["https://coinmarketcap.com/"]

    def parse(self, response):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        rows = response.xpath('//table//tbody/tr')
        for row in rows:
            item = {
                'rank': row.xpath('.//td[1]//text()').get(default='').strip(),
                'name': row.xpath('.//td[3]//a//p[1]/text()').get(default='').strip(),
                'symbol': row.xpath('.//td[3]//a//p[2]/text()').get(default='').strip(),
                'price': row.xpath('.//td[4]//span/text()').get(default='').strip(),
                'market_cap': row.xpath('.//td[10]//span/text()').get(default='').strip(),
                'volume_24h': row.xpath('.//td[8]//span/text()').get(default='').strip(),
                'timestamp': timestamp
            }
            self.save_to_json(item)
            self.save_to_csv(item)

            
        # Descubrimiento de más páginas (navegación autónoma)
        next_page = response.xpath('//a[@aria-label="Next page"]/@href').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)


    def save_to_json(self, item):
        #quitar columna 'timestamp' de la verificación. Siempre viene con datas
        item_limpio = {key: value for key, value in item.items() if key != 'timestamp' and value.strip() != ""}
        #guardar si el item no está vacío
        if item_limpio:           
            if not os.path.exists('../data'):
                os.makedirs('../data')

            filename = "data/coin_data.json"
            data = []
            if os.path.exists(filename):
                with open(filename, "r", encoding="utf-8") as f:
                    try:
                        data = json.load(f)
                    except json.JSONDecodeError:
                        data = []
            data.append(item) 
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)

    def save_to_csv(self, item):
        #quitar columna 'timestamp' de la verificación. Siempre viene con datas
        item_limpio = {key: value for key, value in item.items() if key != 'timestamp' and value.strip() != ""}

        if item_limpio:              
            if not os.path.exists('../data'):
                os.makedirs('../data')

            filename = "data/coin_data.csv"
            file_exists = os.path.exists(filename)
            with open(filename, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=item.keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerow(item)  


    def plot_data_from_json(self):
        #Cargar los datos
        with open('data/coin_data.json', 'r', encoding='utf-8') as file:
            data = json.load(file)        
        prices_by_datetime = {}

        for item in data:            
            timestamp = item.get('timestamp')
            if timestamp:
                try:
                    datetime_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M")
                except ValueError:
                    continue 

                #criptomoneda: nombre y precio
                name = item.get('name')
                price = item.get('price')

                #Limpiar datas
                if price:
                    #conversión float
                    price = float(price.replace('$', '').replace(',', ''))

                    if name not in prices_by_datetime:
                        prices_by_datetime[name] = {}
                    #serie temporal de precio
                    prices_by_datetime[name][datetime_obj] = price

        #el gráfico
        fig = go.Figure()

        for name, price_data in prices_by_datetime.items():
            #sort por criptomoneda
            sorted_dates = sorted(price_data.keys())
            sorted_prices = [price_data[date] for date in sorted_dates]

            fig.add_trace(go.Scatter(
                x=sorted_dates, 
                y=sorted_prices, 
                mode='lines', 
                name=name
            ))

        #titulos y etiquetas
        fig.update_layout(
            title='Datos de criptomonedas de CoinMarketCap: Precios, Volúmenes y Capitalización de Mercado',
            xaxis_title='Date & Time',
            yaxis_title='Precio en USD',
            xaxis_tickangle=-45,
            template='plotly_dark',
        )
        if not os.path.exists('data'):
            os.makedirs('data')
        
        fig.write_html('data/cryptocurrency_price_variations.html')
        fig.show()
        
    def plot_data_from_json2(self):
        #datos desde el archivo JSON
        with open('data/coin_data.json', 'r', encoding='utf-8') as file:
            data = json.load(file)

        #Extraer las listas de valores 
        names = [item['name'] for item in data if item['name'] and item['price']]
        prices = [float(item['price'].replace('$', '').replace(',', '')) for item in data if item['price']]
        market_caps = [float(item['market_cap'].replace('$', '').replace(',', '').replace('M', '').replace('B', '')) for item in data if item['market_cap']]
                
        #names = names[:10]
        #prices = prices[:10]
        #market_caps = market_caps[:10]

        #figura con dos subgráficos
        fig, ax1 = plt.subplots(figsize=(10, 6))

        #Grafico de los precios
        ax1.bar(names, prices, color='blue', alpha=0.6, label='Price ($)')
        ax1.set_xlabel('Cryptocurrency')
        ax1.set_ylabel('Price ($)', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        ax2 = ax1.twinx()
        ax2.plot(names, market_caps, color='green', marker='o', label='Market Cap ($)', linestyle='dashed')
        ax2.set_ylabel('Market Cap ($)', color='green')
        ax2.tick_params(axis='y', labelcolor='green')

        #Título y etiquetas
        plt.title('Top 10 Cryptos: Precio and Market Cap')
        plt.xticks(rotation=45, ha='right')

        #gráfica
        plt.tight_layout()
        plt.show()                


#proceso Scrapy
runner = CrawlerRunner(settings={
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "ROBOTSTXT_OBEY": False,
    "DOWNLOAD_DELAY": 2.0,
    "AUTOTHROTTLE_ENABLED": True,
    "AUTOTHROTTLE_START_DELAY": 1,
    "AUTOTHROTTLE_MAX_DELAY": 3,
})


intervalo = 60  # cada 60 segundos raspar

def run_spider():
    d = runner.crawl(CoinMarketCapSpider)
    d.addCallback(lambda _: CoinMarketCapSpider().plot_data_from_json())
    return d
#ejecutar indefinidamente
looping = task.LoopingCall(run_spider)
looping.start(intervalo)

#(Scrapy + Twisted)
reactor.run()
