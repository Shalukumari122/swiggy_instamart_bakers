import hashlib
import html
import json
import os
import random
import re
import time
import pymysql
import scrapy
from curl_cffi import requests

from scrapy.cmdline import execute
from swiggy_instamart_bakers.items import SwiggyInstamartBakersItem_Product
from swiggy_instamart_bakers.config import *

# client = ScraperAPIClient('64a773e99ca0093e4f80e217a71f821b')

class swiggy_im_spider(scrapy.Spider):
    name = 'swiggy_im_pdp_scraper'
    handle_httpstatus_list = [404]

    def __init__(self,start=0,end=0):
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='actowiz',
            database='swiggy_instamart_bakers',
            # cursorclass=pymysql.cursor.Dictcurosr
        )
        self.cursor = self.conn.cursor()
        self.start=start
        self.end=end

    def start_requests(self):

        def adjust_time():
            # Get the current time
            now = datetime.now()

            # Check the hour and adjust the time accordingly
            if now.hour < 10:
                # Set to 10:00 AM today
                next_time = now.replace(hour=10, minute=0, second=0, microsecond=0)
            elif now.hour < 15:
                # Set to 03:00 PM today
                next_time = now.replace(hour=15, minute=0, second=0, microsecond=0)
            elif now.hour < 20:
                # Set to 08:00 PM today
                next_time = now.replace(hour=20, minute=0, second=0, microsecond=0)
            else:
                # Set to 12:00 AM (midnight) the next day
                next_time = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

            # Format the time as requested
            formatted_time = next_time.strftime("%Y_%m_%d_%I_%p")

            return formatted_time

        today_date_time = adjust_time()

        folder_loc =  f'C:/Shalu/PageSave/swiggy_bakers/page_save_{today_date_time}/'

        if not os.path.exists(folder_loc):
            os.mkdir(folder_loc)

        query = f"SELECT * FROM master_table WHERE status ='pending' and  id BETWEEN {self.start} AND {self.end} "

        # query = "SELECT *FROM master_table where status ='pending' and id=41 "
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        cookies_from_file = json.loads(
            open(r'C:\Users\shalu.kumari\Downloads\get_cookies\swi_cookies.json', 'r').read())
        pxs = [
            "185.188.76.152",
            "104.249.0.116",
            "185.207.96.76",
            "185.205.197.4",
            "185.199.117.103",
            "185.193.74.119",
            "185.188.79.150",
            "185.195.223.146",
            "181.177.78.203",
            "185.207.98.115",
            "186.179.10.253",
            "185.196.189.131",
            "185.205.199.143",
            "185.195.222.22",
            "186.179.20.88",
            "185.188.79.126",
            "185.195.213.198",
            "185.207.98.192",
            "186.179.27.166",
            "181.177.73.165",
            "181.177.64.160",
            "104.233.53.55",
            "185.205.197.152",
            "185.207.98.200",
            "67.227.124.192",
            "104.249.3.200",
            "104.239.114.248",
            "181.177.67.28",
            "185.193.74.7",
            "216.10.5.35",
            "104.233.55.126",
            "185.195.214.89",
            "216.10.1.63",
            "104.249.1.161",
            "186.179.27.91",
            "185.193.75.26",
            "185.195.220.100",
            "185.205.196.226",
            "185.195.221.9",
            "199.168.120.156",
            "181.177.69.174",
            "185.207.98.8",
            "185.195.212.240",
            "186.179.25.90",
            "199.168.121.162",
            "185.199.119.243",
            "181.177.73.168",
            "199.168.121.239",
            "185.195.214.176",
            "181.177.71.233",
            "104.233.55.230",
            "104.249.6.234",
            "104.249.3.87",
            "67.227.125.5",
            "104.249.2.53",
            "181.177.64.15",
            "104.249.7.79",
            "186.179.4.120",
            "67.227.120.39",
            "181.177.68.19",
            "186.179.12.120",
            "104.233.52.54",
            "104.239.117.252",
            "181.177.77.65",
            "185.195.223.56",
            "185.207.99.39",
            "104.249.7.103",
            "185.207.99.11",
            "186.179.3.220",
            "181.177.72.117",
            "185.205.196.180",
            "104.249.2.172",
            "185.207.98.181",
            "185.205.196.255",
            "104.239.113.239",
            "216.10.1.94",
            "181.177.77.2",
            "104.249.6.84",
            "104.239.115.50",
            "185.199.118.209",
            "104.233.55.92",
            "185.207.99.117",
            "104.233.54.71",
            "185.199.119.25",
            "181.177.78.82",
            "104.239.113.76",
            "216.10.7.90",
            "181.177.78.202",
            "104.239.119.189",
            "181.177.64.245",
            "185.199.118.216",
            "185.199.116.219",
            "185.188.77.64",
            "185.199.116.185",
            "185.188.78.176",
            "186.179.12.162",
            "185.205.197.193",
            "181.177.74.161",
            "67.227.126.121",
            "181.177.79.185",

        ]

        for row in rows:
            pincode=row[0]
            area=row[1]
            city=row[2]
            store_id=row[3]
            brand_name=row[4]
            sku_name=row[5]
            product_name=row[6]
            product_id=row[7]
            product_url=row[8]
            hashid = row[11]
            cookies = cookies_from_file[str(pincode)]
            cookies['strId'] = store_id
            # e73b4d0dd39545a3326ed122bedbcf6b

            # concatenated_str = sku_name + area + str(pincode)

            # hashid = hashlib.md5(concatenated_str.encode()).hexdigest()
            self.cursor1=self.conn.cursor()
            # update_query = """
            #         UPDATE master_table
            #         SET hashid = %s
            #         WHERE sku_name = %s AND area = %s AND pincode = %s
            #         """
            # self.cursor.execute(update_query, (hashid, sku_name, area, pincode))
            # self.conn.commit()
            # print("Row updated successfully.")

            if store_id != None:
                sub_folder_loc = folder_loc + f"{pincode}/"
                if not os.path.exists(sub_folder_loc):
                    os.mkdir(sub_folder_loc)


                page_loc = sub_folder_loc + f"{product_id}.html"

                if product_url!= None:

                    url=product_url

                    if not os.path.isfile(page_loc) and not os.path.isfile(page_loc.replace(f"{str(pincode)}/", f"{pincode}/not_found_")):

                        browsers = [
                            "chrome110",
                            "edge99",
                            "safari15_5"
                        ]
                        # meta["impersonate"] = random.choice(browsers)

                        headers = {
                                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                                'accept-language': 'en-US,en;q=0.9',
                                'cache-control': 'no-cache',
                                'pragma': 'no-cache',
                                'priority': 'u=0, i',
                                'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
                                'sec-ch-ua-mobile': '?0',
                                'sec-ch-ua-platform': '"Windows"',
                                'sec-fetch-dest': 'document',
                                'sec-fetch-mode': 'navigate',
                                'sec-fetch-site': 'none',
                                'sec-fetch-user': '?1',
                                'upgrade-insecure-requests': '1',
                                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
                        }

                        time.sleep(1)

                        yield scrapy.Request(
                                             url=str(url),
                                             headers=headers, callback=self.parse, dont_filter=True,
                                             cookies=cookies,

                                             meta={
                                                 'area': area,
                                                 'pincode':pincode,
                                                 'city':city,
                                                 'store_id': store_id,
                                                 'product_url': url,
                                                 'sku_name': sku_name,
                                                 'product_id': product_id,
                                                 'product_name': product_name,
                                                 'page_loc': page_loc,
                                                 'brand_name': brand_name,
                                                 # "proxy": f"http://kunal_santani577-9elgt:QyqTV6XOSp@{random.choice(pxs)}:3199",
                                                 "hashid":hashid,
                                                 # "impersonate":random.choice(browsers)

                                             })


                    else:


                        if os.path.isfile(page_loc):



                            yield scrapy.Request(url = "file://" + page_loc, callback=self.parse, dont_filter=True,
                                                 meta={
                                                     'area': area,
                                                     'pincode':pincode,
                                                     'city':city,
                                                     'store_id': store_id,
                                                     'product_url': url,
                                                     'sku_name': sku_name,
                                                     'product_id': product_id,
                                                     'product_name': product_name,
                                                     'page_loc': page_loc,
                                                     'brand_name': brand_name,
                                                     # "proxyy": False,
                                                     "hashid": hashid,

                                                 })

                        else:


                            yield scrapy.Request(url="file://" + page_loc.replace(f"{str(pincode)}/", f"{str(pincode)}/not_found_"),                                             callback=self.parse, dont_filter=True,
                                                 meta={
                                                     'area': area,
                                                     'pincode': pincode,
                                                     'city': city,
                                                     'store_id': store_id,
                                                     'product_url': url,
                                                     'sku_name': sku_name,
                                                     'product_id': product_id,
                                                     'product_name': product_name,
                                                     'page_loc': page_loc,
                                                     'brand_name': brand_name,
                                                     # "proxyy": False,
                                                     "hashid": hashid

                                                 })

                else:
                    time.sleep(0.5)


                    yield scrapy.Request(url="https://books.toscrape.com/", callback=self.parse, dont_filter=True,
                                         meta={"pincode": pincode,
                                               "area": area,
                                               "city": city,
                                               "store_id": store_id,
                                               'sku_name': sku_name,
                                               'product_url': product_url,
                                               'brand_name': brand_name,

                                               'product_id': product_id,
                                               'product_name': product_name,
                                               "hashid": hashid
                                               }
                                         )
            else:
                time.sleep(0.5)


                yield scrapy.Request(url="https://books.toscrape.com/",callback=self.parse1,dont_filter=True,
                                     meta={"pincode":pincode,
                                       "area":area,
                                       "city":city,
                                       "store_id":store_id,
                                           'sku_name': sku_name,
                                           'product_url':product_url,
                                           'brand_name':brand_name,

                                           'product_id': product_id,
                                           'product_name': product_name,
                                           "hashid":hashid
                                           }
                                 )

    def clean_name(self, value: str):
        if value.strip():
            value = (
                value.strip()
                .replace('\\', '')
                .replace('"', '\"')
                .replace("\u200c", "")
                .replace("\u200f", "")
                .replace("\u200e", "")
            )
            if "\n" in value:
                value = " ".join(value.split())
            return value

    def parse(self, response):
        if response.status == 404:
            request = response.request
            yield request


        if response.xpath('//script[@type= "application/ld+json"]/text()').get() is not None:

            if not os.path.isfile(response.request.meta['page_loc']) and response.status == 200:
                with open(response.request.meta['page_loc'], 'wb') as file:
                    file.write(response.body)

            basic_data = json.loads(response.xpath('//script[@type= "application/ld+json"]/text()').get())

            other_data = json.loads(
                response.xpath('//script[contains(text(), "window.___INITIAL_STATE__")]/text()').get().split(
                    ';  var App')[0].replace('  window.___INITIAL_STATE___ = ', ''))

            for each_variation in other_data['instamart']['cachedProductItemData']['lastItemState']['variations']:

                items = SwiggyInstamartBakersItem_Product()
                items['platform'] = 'SwiggyInstamart'
                items['pincode'] = str(response.request.meta['pincode'])
                date = datetime.now()
                formatted_date = date.strftime("%d-%b-%y")
                items['dateOfScrape']=formatted_date
                items['area'] = response.request.meta['area']#input
                items['city'] = response.request.meta['city']#input
                items['productUrl'] = response.request.meta['product_url']#input
                items['storeid'] = str(response.request.meta['store_id'])#input
                items['lat_long'] = ''
                items['SkuName'] = response.request.meta['sku_name']#input
                items['productId']=response.request.meta['product_id']
                items['BrandName'] =response.request.meta['brand_name']
                items['productName']=response.request.meta['product_name']


                items['productImage'] = 'https://instamart-media-assets.swiggy.com/swiggy/image/upload/fl_lossy,f_auto,q_auto/' + \
                                      each_variation['images'][0]
                items['mrp'] = each_variation['price']['mrp']
                items['productPrice'] = each_variation['price']['offer_price']
                items['discount'] = each_variation['price']['offer_applied']['listing_description']
                items['quantity'] = each_variation['max_allowed_quantity']
                stock=each_variation['inventory']['in_stock']
                if stock==True:
                    items['instock'] = 1
                else:
                    items['instock']=0



                videos = []
                for each_v in each_variation['videos']:
                    videos.append(
                        'https://instamart-media-assets.swiggy.com/swiggy/video/upload/f_auto,q_auto,c_fill,w_1552/' +
                        each_v['src'])

                about_dict = dict()

                if (each_variation['meta']['disclaimer'] != '') or (each_variation['meta']['disclaimer'] is not None):
                    about_dict['Disclaimer'] = each_variation['meta']['disclaimer']

                for each_widget in other_data['instamart']['cachedProductItemData']['widgetsState']:

                    if each_widget['type'] == 'PRODUCT_DETAILS_WIDGET':

                        for each_ab in each_widget['data'][0]['line_items']:
                            about_dict[each_ab['title']] = self.clean_name(html.unescape(
                                re.sub(re.compile('<.*?>'), ' ',
                                       each_ab['description']).strip()))

                    if each_widget['type'] == 'SELLER_DETAILS_WIDGET':

                        for each_sd in each_widget['data'][0]['sellerDetailsList'][0]:
                            # print(each_sd)

                            if 'Seller Name' in each_sd:
                                items['storeName'] = each_sd.replace('Seller Name: ','').strip()
                            if 'Address' in each_sd:
                                items['address'] = each_sd.replace('Address: ','').strip()

                others_dict = {
                    'name': each_variation['display_name'],
                    'brand': other_data['instamart']['cachedProductItemData']['lastItemState']['brand'],
                    'images': ' | '.join(
                        ['https://instamart-media-assets.swiggy.com/swiggy/image/upload/fl_lossy,f_auto,q_auto/'
                         + each for each in each_variation['images']]),
                    'videos': ' | '.join(videos),
                    'about': about_dict,

                }

                items['others'] = json.dumps(others_dict, ensure_ascii=True).replace(r'\u00a0', ' ')
                items['variation_id'] = each_variation['id']
                # items['scraped_time'] = "08:00 PM"
                # items['scraped_time'] = "10:00 AM"
                items['scraped_time'] = "03:00 PM"
                items['CategoryName'] = each_variation['sub_category']
                category_hierarchy = {
                    'l1': each_variation['super_category'],
                    'l2': each_variation['category'],
                    'l3': each_variation['sub_category_type'],
                    'l4': each_variation['sub_category']
                }

                items['category_hierarchy'] = str(category_hierarchy)
                items['hashid']=response.request.meta['hashid']

                yield items


        else:
            page_loc = response.request.meta['page_loc']
            page_loc = page_loc.replace(f"{str(response.request.meta['pincode'])}/",f"{str(response.request.meta['pincode'])}/not_found_")
            if not os.path.isfile(page_loc) and response.status == 200:
                with open(page_loc, 'wb') as file:
                    file.write(response.body)

            items = SwiggyInstamartBakersItem_Product()
            productUrl=response.request.meta['product_url']
            items['platform'] = 'SwiggyInstamart'
            items['pincode'] = str(response.request.meta['pincode'])
            date = datetime.now()
            formatted_date = date.strftime("%d-%b-%y")
            items['dateOfScrape']=formatted_date
            items['area'] = response.request.meta['area']  # input
            items['city'] = response.request.meta['city']
            items['productUrl'] =productUrl
            items['storeid'] = str(response.request.meta['store_id'])  # input
            items['lat_long'] = ''
            items['SkuName'] = response.request.meta['sku_name']  # input
            items['productId'] = response.request.meta['product_id'] # input
            items['BrandName'] = response.request.meta['brand_name']
            items['productName'] = response.request.meta['product_name'] # input
            items['productImage'] = ''
            items['mrp'] = ''
            items['productPrice'] =''
            items['discount'] = ''
            items['quantity'] =''
            items['instock'] = ''
            items['storeName']=''
            items['address']=''
            items['others'] = ''
            items['variation_id'] = ''
            # items['scraped_time'] = "08:00 PM"
            # items['scraped_time'] = "10:00 AM"
            items['scraped_time'] = "03:00 PM"
            items['CategoryName'] = ''
            items['category_hierarchy'] = ''
            items['hashid'] = response.request.meta['hashid']
            yield items
            # if productUrl==None:
            #     yield items
            # else:
            #     print("we can't insert it...")

    def parse1(self,response):

        items = SwiggyInstamartBakersItem_Product()
        items['platform'] = 'SwiggyInstamart'
        items['pincode'] = str(response.request.meta['pincode'])
        date = datetime.now()
        formatted_date = date.strftime("%d-%b-%y")
        items['dateOfScrape'] = formatted_date
        items['area'] = response.request.meta['area']  # input
        items['city'] = response.request.meta['city']
        items['productUrl'] = response.request.meta['product_url']
        items['storeid'] = ''  # input
        items['lat_long'] = ''
        items['SkuName'] = response.request.meta['sku_name']  # input
        items['productId'] = response.request.meta['product_id']  # input
        items['BrandName'] = response.request.meta['brand_name']
        items['productName'] = response.request.meta['product_name']  # input
        items['productImage'] = ''
        items['mrp'] = ''
        items['productPrice'] = ''
        items['discount'] = ''
        items['quantity'] = ''
        items['instock'] = ''
        items['storeName'] = ''
        items['address'] = ''
        items['others'] = ''
        items['variation_id'] = ''
        # items['scraped_time'] = "08:00 PM"
        # items['scraped_time'] = "10:00 AM"
        items['scraped_time'] = "03:00 PM"
        items['CategoryName'] = ''
        items['category_hierarchy'] = ''
        items['hashid'] = response.request.meta['hashid']
        yield items


if __name__ == '__main__':

    def adjust_time():
        # Get the current time
        now = datetime.now()

        # Check the hour and adjust the time accordingly
        if now.hour < 10:
            # Set to 10:00 AM today
            next_time = now.replace(hour=10, minute=0, second=0, microsecond=0)
        elif now.hour < 15:
            # Set to 03:00 PM today
            next_time = now.replace(hour=15, minute=0, second=0, microsecond=0)
        elif now.hour < 20:
            # Set to 08:00 PM today
            next_time = now.replace(hour=20, minute=0, second=0, microsecond=0)
        else:
            # Set to 12:00 AM (midnight) the next day
            next_time = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

        # Format the time as requested
        formatted_time = next_time.strftime("%Y_%m_%d_%I_%p")

        return formatted_time


    today_date_time = adjust_time()

    folder_loc = f'C:/Shalu/LiveProjects/swiggy_instamart_bakers/data_files/{today_date_time}/'


    if not os.path.exists(folder_loc):
        os.mkdir(folder_loc)


    execute(f"scrapy crawl swiggy_im_pdp_scraper  -a start=0 -a end=4264 -O {folder_loc}swiggy_instamart_bakers_{today_date_time}.json".split())

