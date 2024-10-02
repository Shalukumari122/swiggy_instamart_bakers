# import hashlib
# import html
# import json
# import os
# import re
# import time
# import random
#
# import numpy as np
# import pymysql
# import scrapy
# from scraper_api import ScraperAPIClient
# from scrapy.cmdline import execute
# from swiggy_instamart_bakers.items import SwiggyInstamartBakersItem_Product
# from swiggy_instamart_bakers.config import *
# import pandas as pd
#
# # client = ScraperAPIClient('64a773e99ca0093e4f80e217a71f821b')
#
# class swiggy_im_spider(scrapy.Spider):
#     name = 'swiggy_im_pdp_scraper'
#
#     def __init__(self):
#         # Connect to MySQL database
#         self.conn = pymysql.connect(
#             host='localhost',
#             user='root',
#             password='actowiz',
#             database='swiggy_instamart_bakers'
#         )
#         self.cursor = self.conn.cursor()
#     def start_requests(self):
#
#         def adjust_time():
#             # Get the current time
#             now = datetime.now()
#
#             # Check the hour and adjust the time accordingly
#             if now.hour < 10:
#                 # Set to 10:00 AM today
#                 next_time = now.replace(hour=10, minute=0, second=0, microsecond=0)
#             elif now.hour < 15:
#                 # Set to 03:00 PM today
#                 next_time = now.replace(hour=15, minute=0, second=0, microsecond=0)
#             elif now.hour < 20:
#                 # Set to 08:00 PM today
#                 next_time = now.replace(hour=20, minute=0, second=0, microsecond=0)
#             else:
#                 # Set to 12:00 AM (midnight) the next day
#                 next_time = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
#
#             # Format the time as requested
#             formatted_time = next_time.strftime("%Y_%m_%d_%I_%p")
#
#             return formatted_time
#
#         today_date_time = adjust_time()
#
#         products_df = pd.read_excel(r'C:\Shalu\LiveProjects\swiggy_instamart_bakers\input_files\products.xlsx')
#         # stores_df = pd.read_excel(r'C:\Shalu\LiveProjects\swiggy_instamart_bakers\input_files\store.xlsx')
#         folder_loc =  f'C:/Shalu/PageSave/swiggy_bakers/page_save_{today_date_time}/'
#         if not os.path.exists(folder_loc):
#             os.mkdir(folder_loc)
#
#         for product_id, sku_name, product_name in zip(products_df['product_id'], products_df['sku_name'], products_df['product_name']):
#
#             # for each_tup in stores_df.to_records(index=False):
#             query = "SELECT *FROM store"
#             self.cursor.execute(query)
#
#             # Fetch all rows at once
#             rows = self.cursor.fetchall()
#
#             # Iterate through each row in the result set
#             for row in rows:
#                 pincode = row[0]
#                 area = row[1]
#                 city = row[2]
#                 store_id = row[3]
#
#                 if store_id != None:
#                     sub_folder_loc = folder_loc + f"{store_id}/"
#                     if not os.path.exists(sub_folder_loc):
#                         os.mkdir(sub_folder_loc)
#                         # print(sub_folder_loc)
#                     page_loc = sub_folder_loc + f"{product_id}.html"
#
#                     url = f"https://www.swiggy.com/instamart/item/{product_id}?storeId={store_id}"
#
#                     if not os.path.isfile(page_loc) and not os.path.isfile(page_loc.replace(f"{str(store_id)}/", f"{store_id}/not_found_")):
#
#                     # if not os.path.isfile(page_loc) and not os.path.isfile(page_loc.replace(f"{str(store_id)}/",f"{store_id}/not_found_")):
#                         # print(page_loc)
#                         # continue
#                         headers = {
#                             'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
#                             'accept-language': 'en-US,en;q=0.9',
#                             'cache-control': 'no-cache',
#                             # 'cookie': '__SW=W9UCPh5ySrkx3z7LdbLfhfjPan0sU3nh; _device_id=75be40f9-a0c3-4e0e-1c86-d1f2ced0c907; _gcl_au=1.1.406511299.1718791436; _fbp=fb.1.1719233702143.931722654449798998; deviceId=s%3A75be40f9-a0c3-4e0e-1c86-d1f2ced0c907.Avin0k%2Fw1dkeC6Q2CDOF4DL6LvQ0cdWbPdzv0llDeCs; versionCode=1200; platform=web; subplatform=dweb; statusBarHeight=0; bottomOffset=0; genieTrackOn=false; isNative=false; openIMHP=false; addressId=s%3A.4Wx2Am9WLolnmzVcU32g6YaFDw0QbIBFRj2nkO7P25s; webBottomBarHeight=0; _ga_4BQKMMC7Y9=GS1.2.1723546342.9.1.1723546348.54.0.0; _gid=GA1.2.1530605234.1724130474; fontsLoaded=1; lat=s%3A28.5279118.PC7rKzpTQylqQw9CyVn8yO%2Bh9Xv3OOtlSkut%2FNSda2k; lng=s%3A77.20889869999999.1oT4Qa6G%2FxvpwUjLx%2FIV7YXW9Fh3qkLSCgfcqRBe%2FDk; address=s%3ANew%20Delhi%2C%20Delhi%20110017%2C%20India.oqwgnSjwL7vNf%2FPd6%2FBtqKF771PsolXMjg3HyUCzwCs; ally-on=false; strId=; LocSrc=s%3AswgyUL.Dzm1rLPIhJmB3Tl2Xs6141hVZS0ofGP7LGmLXgQOA7Y; tid=s%3Af4439504-8c98-4f9b-b075-6e45a5547948.EApXTMhNvkPDGvhEvgFYTHnZGOua2Wsf2IDUuBro6%2Bs; sid=s%3Afrobe9b8-6afa-4f91-b4c0-0e4693a9a658.UDqYZxsk0qAqeC1kfL2xYcTcrhoPffY%2FnN0DKY3HnTQ; _guest_tid=6fffab72-2542-44ea-b4ae-ee1483a3e66c; _sid=frqb71a0-896b-44f2-aae3-a9929c4e8938; _ga_34JYJ0BCRN=GS1.1.1724749342.60.0.1724749342.0.0.0; _ga=GA1.1.1943441830.1724601204; userLocation=%7B%22address%22%3A%22New%20Delhi%2C%20Delhi%20110017%2C%20India%22%2C%22lat%22%3A28.5279118%2C%22lng%22%3A77.20889869999999%2C%22id%22%3A%22%22%2C%22annotation%22%3A%22%22%2C%22name%22%3A%22%22%7D; imOrderAttribution={%22entryId%22:null%2C%22entryName%22:null%2C%22entryContext%22:null%2C%22hpos%22:null%2C%22vpos%22:null%2C%22utm_source%22:null%2C%22utm_medium%22:null%2C%22utm_campaign%22:null}; _ga_8N8XRG907L=GS1.1.1724748816.15.1.1724752649.0.0.0',
#                             'pragma': 'no-cache',
#                             'priority': 'u=0, i',
#                             'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
#                             'sec-ch-ua-mobile': '?0',
#                             'sec-ch-ua-platform': '"Windows"',
#                             'sec-fetch-dest': 'document',
#                             'sec-fetch-mode': 'navigate',
#                             'sec-fetch-site': 'none',
#                             'sec-fetch-user': '?1',
#                             'upgrade-insecure-requests': '1',
#                             'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
#                         }
#                         cookies = {
#                             '__SW': 'W9UCPh5ySrkx3z7LdbLfhfjPan0sU3nh',
#                             '_device_id': '75be40f9-a0c3-4e0e-1c86-d1f2ced0c907',
#                             '_gcl_au': '1.1.406511299.1718791436',
#                             '_fbp': 'fb.1.1719233702143.931722654449798998',
#                             'deviceId': 's%3A75be40f9-a0c3-4e0e-1c86-d1f2ced0c907.Avin0k%2Fw1dkeC6Q2CDOF4DL6LvQ0cdWbPdzv0llDeCs',
#                             'versionCode': '1200',
#                             'platform': 'web',
#                             'subplatform': 'dweb',
#                             'statusBarHeight': '0',
#                             'bottomOffset': '0',
#                             'genieTrackOn': 'false',
#                             'isNative': 'false',
#                             'openIMHP': 'false',
#                             'addressId': 's%3A.4Wx2Am9WLolnmzVcU32g6YaFDw0QbIBFRj2nkO7P25s',
#                             'webBottomBarHeight': '0',
#                             '_ga_4BQKMMC7Y9': 'GS1.2.1723546342.9.1.1723546348.54.0.0',
#                             '_gid': 'GA1.2.1530605234.1724130474',
#                             'fontsLoaded': '1',
#                             # 'lat': 's%3A28.5279118.PC7rKzpTQylqQw9CyVn8yO%2Bh9Xv3OOtlSkut%2FNSda2k',
#                             # 'lng': 's%3A77.20889869999999.1oT4Qa6G%2FxvpwUjLx%2FIV7YXW9Fh3qkLSCgfcqRBe%2FDk',
#                             'address': 's%3ANew%20Delhi%2C%20Delhi%20110017%2C%20India.oqwgnSjwL7vNf%2FPd6%2FBtqKF771PsolXMjg3HyUCzwCs',
#                             'ally-on': 'false',
#                             'strId': '',
#                             'LocSrc': 's%3AswgyUL.Dzm1rLPIhJmB3Tl2Xs6141hVZS0ofGP7LGmLXgQOA7Y',
#                             'tid': 's%3Af4439504-8c98-4f9b-b075-6e45a5547948.EApXTMhNvkPDGvhEvgFYTHnZGOua2Wsf2IDUuBro6%2Bs',
#                             'sid': 's%3Afrobe9b8-6afa-4f91-b4c0-0e4693a9a658.UDqYZxsk0qAqeC1kfL2xYcTcrhoPffY%2FnN0DKY3HnTQ',
#                             '_guest_tid': '6fffab72-2542-44ea-b4ae-ee1483a3e66c',
#                             '_sid': 'frqb71a0-896b-44f2-aae3-a9929c4e8938',
#                             '_ga_34JYJ0BCRN': 'GS1.1.1724749342.60.0.1724749342.0.0.0',
#                             '_ga': 'GA1.1.1943441830.1724601204',
#                             # 'userLocation': '%7B%22address%22%3A%22New%20Delhi%2C%20Delhi%20110017%2C%20India%22%2C%22lat%22%3A28.5279118%2C%22lng%22%3A77.20889869999999%2C%22id%22%3A%22%22%2C%22annotation%22%3A%22%22%2C%22name%22%3A%22%22%7D',
#                             'imOrderAttribution': '{%22entryId%22:null%2C%22entryName%22:null%2C%22entryContext%22:null%2C%22hpos%22:null%2C%22vpos%22:null%2C%22utm_source%22:null%2C%22utm_medium%22:null%2C%22utm_campaign%22:null}',
#                             '_ga_8N8XRG907L': 'GS1.1.1724748816.15.1.1724752649.0.0.0',
#                         }
#
#                         time.sleep(0.2)
#                         yield scrapy.Request(
#                                              url=url,
#                                              # url=client.scrapyGet(url=url,headers=headers),
#                                              headers=headers, callback=self.parse, dont_filter=True,
#                                              cookies=cookies,
#                                              meta={
#                                                  'area': area,
#                                                  'pincode':pincode,
#                                                  'city':city,
#                                                  'store_id': store_id,
#                                                  'product_url': url,
#                                                  'sku_name': sku_name,
#                                                  # 'slot': slot,
#                                                  'product_id': product_id,
#                                                  'product_name': product_name,
#                                                  'page_loc': page_loc,
#                                                  # "proxyy":True,
#                                                  # "unique_id":unique_id
#                                                  # "proxy": "http://scraperapi:64a773e99ca0093e4f80e217a71f821b@proxy-server.scraperapi.com:8001"
#
#                                              })
#
#
#                     else:
#
#
#                         if os.path.isfile(page_loc):
#
#
#                             yield scrapy.Request(url = "file://" + page_loc, callback=self.parse, dont_filter=True,
#                                                  # cookies=random.choice([cookies_1, cookies_2, cookies_3]),
#                                                  meta={
#                                                      'area': area,
#                                                      'pincode':pincode,
#                                                      'city':city,
#                                                      'store_id': store_id,
#                                                      'product_url': url,
#                                                      'sku_name': sku_name,
#                                                      # 'slot': slot,
#                                                      'product_id': product_id,
#                                                      'product_name': product_name,
#                                                      'page_loc': page_loc,
#                                                      # "proxyy": False,
#                                                      # "unique_id": unique_id
#                                                      # "proxy": "http://scraperapi:64a773e99ca0093e4f80e217a71f821b@proxy-server.scraperapi.com:8001"
#                                                  })
#
#                         else:
#
#                             yield scrapy.Request(url="file://" + page_loc.replace(f"{str(store_id)}/", f"{str(store_id)}/not_found_"),                                             callback=self.parse, dont_filter=True,
#                                                  meta={
#                                                      'area': area,
#                                                      'pincode': pincode,
#                                                      'city': city,
#                                                      'store_id': store_id,
#                                                      'product_url': url,
#                                                      'sku_name': sku_name,
#                                                      # 'slot': slot,
#                                                      'product_id': product_id,
#                                                      'product_name': product_name,
#                                                      'page_loc': page_loc,
#                                                      # "proxyy": False,
#                                                      # "unique_id": unique_id
#
#                                                      # "proxy": "http://scraperapi:64a773e99ca0093e4f80e217a71f821b@proxy-server.scraperapi.com:8001"
#                                                  })
#                 #     break
#                 # break
#                 else:
#                     yield scrapy.Request(url="https://www.google.com/",callback=self.parse,dont_filter=True,
#                                          meta={"pincode":pincode,
#                                            "area":area,
#                                            "city":city,
#                                            "store_id":store_id,
#                                                'sku_name': sku_name,
#                                                # 'slot': slot,
#                                                'product_id': product_id,
#                                                'product_name': product_name
#                                                }
#                                      )
#
#     def clean_name(self, value: str):
#         if value.strip():
#             value = (
#                 value.strip()
#                 .replace('\\', '')
#                 .replace('"', '\"')
#                 .replace("\u200c", "")
#                 .replace("\u200f", "")
#                 .replace("\u200e", "")
#             )
#             if "\n" in value:
#                 value = " ".join(value.split())
#             return value
#
#     def parse(self, response):
#
#         # return
#         print()
#         try:
#
#             if response.xpath('//script[@type= "application/ld+json"]/text()').get() is not None:
#
#                 if not os.path.isfile(response.request.meta['page_loc']) and response.status == 200:
#                     with open(response.request.meta['page_loc'], 'wb') as file:
#                         file.write(response.body)
#
#                 basic_data = json.loads(response.xpath('//script[@type= "application/ld+json"]/text()').get())
#
#                 other_data = json.loads(
#                     response.xpath('//script[contains(text(), "window.___INITIAL_STATE__")]/text()').get().split(
#                         ';  var App')[0].replace('  window.___INITIAL_STATE___ = ', ''))
#                 # print(len(other_data['instamart']['cachedProductItemData']['lastItemState']['variations']))
#
#                 for each_variation in other_data['instamart']['cachedProductItemData']['lastItemState']['variations']:
#
#                     items = SwiggyInstamartBakersItem_Product()
#                     items['platform'] = 'SwiggyInstamart'
#                     items['pincode'] = str(response.request.meta['pincode'])
#                     # items['dateOfScrape'] = today_date()
#                     date = datetime.now()
#                     formatted_date = date.strftime("%d-%b-%y")
#                     items['dateOfScrape']=formatted_date
#                     items['area'] = response.request.meta['area']#input
#                     items['city'] = response.request.meta['city']#input
#                     items['productUrl'] = response.request.meta['product_url']#input
#                     items['storeid'] = str(response.request.meta['store_id'])#input
#                     items['lat_long'] = ''
#                     # items['slot'] = response.request.meta['slot'] #input
#                     items['SkuName'] = response.request.meta['sku_name']#input
#                     items['productId'] = other_data['instamart']['cachedProductItemData']['key']
#                     items['BrandName'] = other_data['instamart']['cachedProductItemData']['lastItemState']['brand']
#                     items['productName'] = each_variation['display_name']
#
#
#                     items['productImage'] = 'https://instamart-media-assets.swiggy.com/swiggy/image/upload/fl_lossy,f_auto,q_auto/' + \
#                                           each_variation['images'][0]
#                     items['mrp'] = each_variation['price']['mrp']
#                     items['productPrice'] = each_variation['price']['offer_price']
#                     items['discount'] = each_variation['price']['offer_applied']['listing_description']
#                     items['quantity'] = each_variation['max_allowed_quantity']
#                     stock=each_variation['inventory']['in_stock']
#                     if stock==True:
#                         items['instock'] = 1
#                     else:
#                         items['instock']=0
#
#
#
#                     videos = []
#                     for each_v in each_variation['videos']:
#                         videos.append(
#                             'https://instamart-media-assets.swiggy.com/swiggy/video/upload/f_auto,q_auto,c_fill,w_1552/' +
#                             each_v['src'])
#
#                     about_dict = dict()
#
#                     if (each_variation['meta']['disclaimer'] != '') or (each_variation['meta']['disclaimer'] is not None):
#                         about_dict['Disclaimer'] = each_variation['meta']['disclaimer']
#
#                     for each_widget in other_data['instamart']['cachedProductItemData']['widgetsState']:
#
#                         if each_widget['type'] == 'PRODUCT_DETAILS_WIDGET':
#
#                             for each_ab in each_widget['data'][0]['line_items']:
#                                 about_dict[each_ab['title']] = self.clean_name(html.unescape(
#                                     re.sub(re.compile('<.*?>'), ' ',
#                                            each_ab['description']).strip()))
#
#                         if each_widget['type'] == 'SELLER_DETAILS_WIDGET':
#
#                             for each_sd in each_widget['data'][0]['sellerDetailsList'][0]:
#                                 # print(each_sd)
#
#                                 if 'Seller Name' in each_sd:
#                                     items['storeName'] = each_sd.replace('Seller Name: ','').strip()
#                                 if 'Address' in each_sd:
#                                     items['address'] = each_sd.replace('Address: ','').strip()
#
#                     others_dict = {
#                         'name': each_variation['display_name'],
#                         'brand': other_data['instamart']['cachedProductItemData']['lastItemState']['brand'],
#                         'images': ' | '.join(
#                             ['https://instamart-media-assets.swiggy.com/swiggy/image/upload/fl_lossy,f_auto,q_auto/'
#                              + each for each in each_variation['images']]),
#                         'videos': ' | '.join(videos),
#                         'about': about_dict,
#
#                     }
#
#                     items['others'] = json.dumps(others_dict, ensure_ascii=True).replace(r'\u00a0', ' ')
#                     items['variation_id'] = each_variation['id']
#                     # items['scraped_time'] = "08:00 PM"
#                     items['scraped_time'] = "10:00 AM"
#                     # items['scraped_time'] = "03:00 PM"
#                     items['CategoryName'] = each_variation['sub_category']
#                     category_hierarchy = {
#                         'l1': each_variation['super_category'],
#                         'l2': each_variation['category'],
#                         'l3': each_variation['sub_category_type'],
#                         'l4': each_variation['sub_category']
#                     }
#
#                     items['category_hierarchy'] = str(category_hierarchy)
#                     # items['unique_id']=response.meta.get('unique_id')
#
#                     yield items
#
#             else:
#                 page_loc = response.request.meta['page_loc']
#                 page_loc = page_loc.replace(f"{str(response.request.meta['store_id'])}/",f"{str(response.request.meta['store_id'])}/not_found_")
#                 if not os.path.isfile(page_loc) and response.status == 200:
#                     with open(page_loc, 'wb') as file:
#                         file.write(response.body)
#
#                 items = SwiggyInstamartBakersItem_Product()
#                 items['platform'] = 'SwiggyInstamart'
#                 items['pincode'] = str(response.request.meta['pincode'])
#                 # items['dateOfScrape'] = today_date()
#                 date = datetime.now()
#                 formatted_date = date.strftime("%d-%b-%y")
#                 items['dateOfScrape']=formatted_date
#                 items['area'] = response.request.meta['area']  # input
#                 items['city'] = response.request.meta['city']
#                 items['productUrl'] = response.request.meta['product_url']
#                 items['storeid'] = str(response.request.meta['store_id'])  # input
#                 items['lat_long'] = ''
#                 items['SkuName'] = response.request.meta['sku_name']  # input
#                 items['productId'] = response.request.meta['product_id'] # input
#                 items['BrandName'] = ''
#                 items['productName'] = response.request.meta['product_name'] # input
#                 items['productImage'] = ''
#                 items['mrp'] = ''
#                 items['productPrice'] =''
#                 items['discount'] = ''
#                 items['quantity'] =''
#                 items['instock'] = ''
#                 items['storeName']=''
#                 items['address']=''
#                 items['others'] = ''
#                 items['variation_id'] = ''
#                 # items['scraped_time'] = "08:00 PM"
#                 items['scraped_time'] = "10:00 AM"
#                 # items['scraped_time'] = "03:00 PM"
#                 items['CategoryName'] = ''
#                 items['category_hierarchy'] = ''
#                 yield items
#
#         except Exception as e:
#             items = SwiggyInstamartBakersItem_Product()
#             items['platform'] = 'SwiggyInstamart'
#             items['pincode'] = str(response.request.meta['pincode'])
#             # items['dateOfScrape'] = today_date()
#             date = datetime.now()
#             formatted_date = date.strftime("%d-%b-%y")
#             items['dateOfScrape'] = formatted_date
#             items['area'] = response.request.meta['area']  # input
#             items['city'] = response.request.meta['city']
#             items['productUrl'] = ''
#             items['storeid'] =  ''# input
#             items['lat_long'] = ''
#             items['SkuName'] = response.request.meta['sku_name']  # input
#             items['productId'] = response.request.meta['product_id']  # input
#             items['BrandName'] = ''
#             items['productName'] = response.request.meta['product_name']  # input
#             items['productImage'] = ''
#             items['mrp'] = ''
#             items['productPrice'] = ''
#             items['discount'] = ''
#             items['quantity'] = ''
#             items['instock'] = ''
#             items['storeName'] = ''
#             items['address'] = ''
#             items['others'] = ''
#             items['variation_id'] = ''
#             # items['scraped_time'] = "08:00 PM"
#             items['scraped_time'] = "10:00 AM"
#             # items['scraped_time'] = "03:00 PM"
#             items['CategoryName'] = ''
#             items['category_hierarchy'] = ''
#             yield items
#
#
# if __name__ == '__main__':
#
#     # today_date = str(date.today()).replace('-', '_')
#     def adjust_time():
#         # Get the current time
#         now = datetime.now()
#
#         # Check the hour and adjust the time accordingly
#         if now.hour < 10:
#             # Set to 10:00 AM today
#             next_time = now.replace(hour=10, minute=0, second=0, microsecond=0)
#         elif now.hour < 15:
#             # Set to 03:00 PM today
#             next_time = now.replace(hour=15, minute=0, second=0, microsecond=0)
#         elif now.hour < 20:
#             # Set to 08:00 PM today
#             next_time = now.replace(hour=20, minute=0, second=0, microsecond=0)
#         else:
#             # Set to 12:00 AM (midnight) the next day
#             next_time = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
#
#         # Format the time as requested
#         formatted_time = next_time.strftime("%Y_%m_%d_%I_%p")
#
#         return formatted_time
#
#
#     today_date_time = adjust_time()
#
#     folder_loc = f'C:/Shalu/LiveProjects/swiggy_instamart_bakers/data_files/{today_date_time}/'
#
#
#     if not os.path.exists(folder_loc):
#         os.mkdir(folder_loc)
#
#
#     execute(f"scrapy crawl swiggy_im_pdp_scraper -O {folder_loc}swiggy_instamart_bakers_{today_date_time}.json".split())
#     # execute(f"scrapy crawl swiggy_im_pdp_scraper ".split())
#
