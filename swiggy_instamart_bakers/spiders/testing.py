import hashlib
import json
import os
import re
import html
from datetime import datetime

import pymysql
# import requests
import scrapy
from curl_cffi import requests
from scrapy.cmdline import execute

from swiggy_instamart_bakers.items import SwiggyInstamartBakersItem_Product1


class TestingSpider(scrapy.Spider):
    name = "testing"
    # allowed_domains = ["google.com"]
    # start_urls = ["https://google.com"]

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
        query = f"SELECT * FROM testing where status='pending' limit 1"

        # query = "SELECT *FROM master_table where status ='pending' and id=1101 "
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        cookies_from_file = json.loads(
            open(r'C:\Users\shalu.kumari\Downloads\get_cookies\swi_cookies_testing.json', 'r').read())

        folder_loc = 'C:/Shalu/pageSave/swiggy_bakers/testing/'

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
            cookies = cookies_from_file[str(pincode)]
            concatenated_str = sku_name + area + str(pincode)
            hashid = hashlib.md5(concatenated_str.encode()).hexdigest()
            update_query = """
                    UPDATE testing
                    SET hashid = %s
                    WHERE sku_name = %s AND area = %s AND pincode = %s
                    """
            self.cursor.execute(update_query, (hashid, sku_name, area, pincode))
            self.conn.commit()
            print("Row updated successfully.")

            self.cursor1 = self.conn.cursor()
            sub_folder_loc = folder_loc + f"{pincode}/"
            if not os.path.exists(sub_folder_loc):
                os.mkdir(sub_folder_loc)

            page_loc = sub_folder_loc + f"{product_id}.html"

            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'no-cache',
                # 'cookie': 'deviceId=s%3A0d3c860d-98e9-4885-820a-493c1d491617.XF%2Bqs073HqMqKw4VikEEi2bTxccKKJHoph%2BcfbT549E; versionCode=1200; platform=web; subplatform=dweb; statusBarHeight=0; bottomOffset=0; genieTrackOn=false; isNative=false; openIMHP=false; webBottomBarHeight=0; _gcl_au=1.1.1889098395.1725677994; _fbp=fb.1.1725677994411.713616407873301799; __SW=oYRfPF5pm3iI5Krmq9MhDadpbNXdZGcF; _device_id=44c65bda-781d-3fb4-5e02-3cc702c6fce2; fontsLoaded=1; ally-on=false; strId=; addressId=s%3A.4Wx2Am9WLolnmzVcU32g6YaFDw0QbIBFRj2nkO7P25s; LocSrc=s%3AswgyUL.Dzm1rLPIhJmB3Tl2Xs6141hVZS0ofGP7LGmLXgQOA7Y; _gid=GA1.2.883193687.1725885842; dadl=true; tid=s%3A9f1c9b0d-3c62-4c0d-9c2b-66dc92850546.w3FtySfrHKdA6nmYln8T%2BlNFIcctwXWbuX5%2FWVa2rD4; sid=s%3Ag1297dc1-4351-4f58-b6a9-c2fda6019b85.5Y9Nb%2B4y7oLgWzNl4cDqkPrPF11fUIIMc%2Bw55llnIYg; _guest_tid=b25e315e-822e-4c83-9db3-978ff25b9bbe; _sid=g138591f-da3e-40a4-afb7-413d03e27d47; _gat_0=1; _ga=GA1.1.1213914408.1725884530; lat=s%3A17.465975.wTFnDjjndskCzMqstsNIxvep5GwuskinwbezUIl%2FT38; lng=s%3A78.4761359.FWSya58ltOkzSOOHmmCsK4ftR%2FA6ooWWmE3W%2FgvX0N8; address=s%3AHyderabad%2C%20Telangana%20500011%2C%20India.zkH5Si9To8burQ%2FE4%2BvN5qLFjtDkVaKG2KErgHFvqgU; userLocation=%7B%22address%22%3A%22Hyderabad%2C%20Telangana%20500011%2C%20India%22%2C%22lat%22%3A17.465975%2C%22lng%22%3A78.4761359%2C%22id%22%3A%22%22%2C%22annotation%22%3A%22%22%2C%22name%22%3A%22%22%7D; _ga_34JYJ0BCRN=GS1.1.1725962845.2.0.1725962858.0.0.0; _ga_8N8XRG907L=GS1.1.1725962837.6.1.1725962859.0.0.0',
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

            response = requests.get(url=f'{product_url}',
                cookies=cookies,
                headers=headers
            )
            # if response.xpath('//script[@type= "application/ld+json"]/text()').get() is not None:

            if not os.path.isfile(page_loc) and response.status_code == 200:
                with open(page_loc, 'wb') as file:
                    file.write(response.text.encode('utf-8'))

            yield scrapy.Request(url="file://" + page_loc, callback=self.parse,
                                 meta={"pincode":pincode,
                                       "area":area,
                                       "city":city,
                                       "store_id":store_id,
                                       "brand_name":brand_name,
                                       "sku_name":sku_name,
                                       "product_name":product_name,
                                       "product_id":product_id,
                                       "product_url":product_url,
                                       "hashid":hashid},
                                 dont_filter=True)

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
        print(response.text)
        if response.xpath('//script[@type= "application/ld+json"]/text()').get() is not None:

            # if not os.path.isfile(response.request.meta['page_loc']) and response.status == 200:
            #     with open(response.request.meta['page_loc'], 'wb') as file:
            #         file.write(response.body)

            basic_data = json.loads(response.xpath('//script[@type= "application/ld+json"]/text()').get())

            other_data = json.loads(
                response.xpath('//script[contains(text(), "window.___INITIAL_STATE__")]/text()').get().split(
                    ';  var App')[0].replace('  window.___INITIAL_STATE___ = ', ''))

            for each_variation in other_data['instamart']['cachedProductItemData']['lastItemState']['variations']:

                items = SwiggyInstamartBakersItem_Product1()
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




if __name__ == '__main__':
    execute('scrapy crawl testing'.split())
