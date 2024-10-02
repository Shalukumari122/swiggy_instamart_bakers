# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import date

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymysql
from swiggy_instamart_bakers.config import *

from swiggy_instamart_bakers.items import SwiggyInstamartBakersItem_Product, SwiggyInstamartBakersItem_Product1


class SwiggyInstamartBakersPipeline:
    def process_item(self, item, spider):
        return item

class mySQldb:

    def __init__(self):
        # Connect to MySQL database
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='actowiz',
            database='swiggy_instamart_bakers'
        )
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        # Define SQL query to create the table

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

        if isinstance(item, SwiggyInstamartBakersItem_Product):

            try:
                query = f"""
                    CREATE TABLE IF NOT EXISTS product_data{today_date_time} (
                    `Sr.No` int auto_increment primary key
                    )
                """

                self.cursor.execute(query)

                self.cursor.execute(f"SHOW COLUMNS FROM product_data{today_date_time}")

                existing_columns = [column[0] for column in self.cursor.fetchall()]
                item_columns = [column_name for column_name
                                in
                                item.keys()]

                for column_name in item_columns:
                    if column_name not in existing_columns:
                        column_name = column_name
                        try:
                            self.cursor.execute(f"ALTER TABLE product_data{today_date_time} ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(e)
            except Exception as e:
                print(e)

            try:

                field_list = []
                value_list = []

                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')

                fields = ','.join(field_list)
                values = ", ".join(value_list)

                insert_query = f" INSERT into product_data{today_date_time} ( " + fields + " ) values ( " + values + " )"

                self.cursor.execute(insert_query, tuple(item.values()))

                self.conn.commit()

            except Exception as e:
                print(e)

            try:
                # Update `master_table` status
                if 'hashid' in item:
                    update_query = "UPDATE master_table SET status = 'Done' WHERE hashid = %s"
                    self.cursor.execute(update_query, (item['hashid'],))
                    self.conn.commit()
                else:
                    print("HashID not found in item.")
            except Exception as e:
                print(f"Error updating master_table: {e}")


        if isinstance(item, SwiggyInstamartBakersItem_Product1):

            try:
                query = f"""
                    CREATE TABLE IF NOT EXISTS testing{today_date_time} (
                    `Sr.No` int auto_increment primary key
                    )
                """

                self.cursor.execute(query)

                self.cursor.execute(f"SHOW COLUMNS FROM testing{today_date_time}")

                existing_columns = [column[0] for column in self.cursor.fetchall()]
                item_columns = [column_name for column_name
                                in
                                item.keys()]

                for column_name in item_columns:
                    if column_name not in existing_columns:
                        column_name = column_name
                        try:
                            self.cursor.execute(f"ALTER TABLE testing{today_date_time} ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(e)
            except Exception as e:
                print(e)

            try:

                field_list = []
                value_list = []

                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')

                fields = ','.join(field_list)
                values = ", ".join(value_list)

                insert_query = f" INSERT into testing{today_date_time} ( " + fields + " ) values ( " + values + " )"

                self.cursor.execute(insert_query, tuple(item.values()))

                self.conn.commit()

            except Exception as e:
                print(e)

            try:
                # Update `master_table` status
                if 'hashid' in item:
                    update_query = "UPDATE testing SET status = 'Done' WHERE hashid = %s"
                    self.cursor.execute(update_query, (item['hashid'],))
                    self.conn.commit()
                else:
                    print("HashID not found in item.")
            except Exception as e:
                print(f"Error updating master_table: {e}")


        return item