from scrapy import Request, Spider
from scrapy.crawler import CrawlerProcess
import pandas as pd
import os
import random
import logging
from fake_useragent import UserAgent
from openpyxl.utils.dataframe import dataframe_to_rows

class MySpider(Spider):
    name = 'se_spider'
    start_urls = []

    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
    }

    def __init__(self):
        self.df = pd.read_excel("C:\\Users\\ywu47\\se_tags\\se_tags\\tags_all_info.xlsx", dtype=str)
        self.df.fillna('', inplace=True)
        self.parsed_df = pd.read_excel("parsed_data.xlsx", dtype=str)
        self.df = self.df[~self.df[['TagName', 'site']].isin(self.parsed_df[['TagName', 'site']]).all(axis=1)]
        self.df = self.df[::-1]
        self.df['url'] = self.df.apply(lambda row: f"https://{row.site}.stackexchange.com/tags/{row.TagName}/info", axis=1)
        self.df['p1'] = ''
        self.df['p2'] = ''
        self.df['p3'] = ''
        self.processed_indices = []

    def start_requests(self):
        user_agent = UserAgent().random
        referers = [
            "https://stackoverflow.com/",
            "https://google.com/",
            "https://bing.com/",
            "https://duckduckgo.com/",
            "https://yahoo.com/",
            "https://baidu.com/",
            "https://yandex.com/",
        ]
        
        for _, row in self.df.iterrows():
            url = row['url']
            referer = random.choice(referers)
            headers = {
                'User-Agent': user_agent,
                'Referer': referer
            }
            yield Request(url, headers=headers, callback=self.parse, dont_filter=True)

    def parse(self, response):
        try:
            url = response.url
            paragraphs = response.xpath('//div[@id="questions"]/div[@class="s-prose js-post-body"]/p')
            parsed_paragraphs = [p.xpath('.//text()').get().strip() for p in paragraphs[:3]]

            row_index = self.df[self.df['url'] == url].index[0]
            self.df.at[row_index, 'p1'] = parsed_paragraphs[0] if len(parsed_paragraphs) > 0 else None
            self.df.at[row_index, 'p2'] = parsed_paragraphs[1] if len(parsed_paragraphs) > 1 else None
            self.df.at[row_index, 'p3'] = parsed_paragraphs[2] if len(parsed_paragraphs) > 2 else None

            self.processed_indices.append(row_index)

            if len(self.processed_indices) >= 1000:
                self.save_data()

        except Exception as e:
            logging.error(f"An error occurred while parsing: {e}")

    def save_data(self):
        excel_file = 'parsed_data.xlsx'
        if self.processed_indices:
            # Get only the rows that have been processed
            chunk_df = self.df.loc[self.processed_indices]

            if not os.path.isfile(excel_file):
                chunk_df.to_excel(excel_file, index=False, header=True)
            else:
                with pd.ExcelWriter(excel_file, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
                    workbook = writer.book
                    sheet = writer.sheets['Sheet1']
                    max_row = sheet.max_row

                    for row in dataframe_to_rows(chunk_df, index=False, header=False):
                        sheet.append(row)

            # Clear the list of processed indices
            self.processed_indices.clear()

    def close(self, reason):
        self.save_data()

# Configure logging
logging.basicConfig(filename='scrapy_log.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Run Scrapy from script
process = CrawlerProcess()

# Run the spider
process.crawl(MySpider)
process.start()
