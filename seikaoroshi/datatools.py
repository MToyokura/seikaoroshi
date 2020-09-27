from pathlib import Path
import pandas
import sqlite3

# market_name_abbrv = {
#     "morioka": "盛岡市中央卸売市場",
#     "sendai": "仙台市中央卸売市場",
#     "akita": "秋田市公設地方卸売市場",
#     "mito": "水戸市公設地方卸売市場",
#     "utsunomiya": "宇都宮市中央卸売市場",
#     "toyosu": "東京都中央卸売市場豊洲市場",
#     "oota": "東京都中央卸売市場大田市場",
#     "toshima": "東京都中央卸売市場豊島市場",
#     "yodobashi": "東京都中央卸売市場淀橋市場",
#     "yokohama": "横浜市中央卸売市場本場",
#     "niigata": "新潟市中央卸売市場",
#     "kanazawa": "金沢市中央卸売市場",
#     "nagano": "長野市（在）地方卸売市場",
#     "gifu": "岐阜市中央卸売市場",
#     "nagoya_main": "名古屋市中央卸売市場本場",
#     "nagoya_north": "名古屋市中央卸売市場北部市場",
#     "kyoto": "京都市中央卸売市場",
#     "osaka_main": "大阪市中央卸売市場本場",
#     "osaka_east": "大阪市中央卸売市場東部市場",
#     "kobe": "神戸市中央卸売市場本場",
#     "okayama": "岡山市中央卸売市場",
#     "hiroshima": "広島市中央卸売市場中央市場",
#     "takamatsu": "高松市中央卸売市場",
#     "matsuyama": "松山市中央卸売市場",
#     "kitakyushu": "北九州市中央卸売市場",
#     "fukuoka": "福岡市中央卸売市場",
#     "kumamoto": "熊本市（在）地方卸売市場",
#     "kagoshima": "鹿児島市中央卸売市場",
#     "okinawa": "沖縄県中央卸売市場",
#     "sapporo": "札幌市中央卸売市場",
# }


class ConnectToDatabase:
    """This creates an instance of a database connection."""

    def __init__(self, db_file_path):
        self.conn = sqlite3.connect(db_file_path)
        self.cur = self.conn.cursor()

    def get_df(self, sql_query):
        """Returns a dataframe corresponding to the result set of the SQL query string."""
        dataframe = pandas.read_sql_query(sql_query, self.conn)
        return dataframe

    def easy_get_df(
        self,
        table=None,
        year=None,
        month=None,
        day=None,
        day_of_week=None,
        market_name=None,
        market_id=None,
        product_name=None,
        product_id=None,
        region=None,
        region_id=None,
        item_total=None,
        order_amount=None,
        high_price=None,
        mid_price=None,
        low_price=None,
        product_grade=None,
        product_class=None,
        brand=None,
        unit=None,
        overview=None,
        trend=None,
        sha256_hexdigest=None,
    ):
        """Returns a dataframe with simple AND filtering of given arguments."""
        # Japanese inputs do not behave as expected!
        # Must add parentheses in future build...
        kwargs_dict = {
            "年": year,
            "月": month,
            "日": day,
            "曜日": day_of_week,
            "市場名": market_name,
            "市場コード": market_id,
            "品目名": product_name,
            "品目コード": product_id,
            "産地名": region,
            "産地コード": region_id,
            "品目計": item_total,
            "入荷量": order_amount,
            "高値": high_price,
            "中値": mid_price,
            "安値": low_price,
            "等級": product_grade,
            "階級": product_class,
            "品名": brand,
            "量目": unit,
            "概況": overview,
            "動向": trend,
            "sha256_hexdigest": sha256_hexdigest,
        }
        # Create a list of arguments with None.
        none_keys = []
        for key, value in kwargs_dict.items():
            if value == None:
                none_keys.append(key)
        # Delete arguments with None from kwargs_dict
        for i in none_keys:
            del kwargs_dict[i]
        # Convert values to string.
        for key in kwargs_dict:
            kwargs_dict[key] = str(kwargs_dict[key])

        # Generate SQL query string
        query_string = f"SELECT * FROM {table} "
        if len(query_string) == 0:
            pass
        else:
            query_string += "WHERE "
            for key in kwargs_dict:
                query_string += f"{key}={kwargs_dict[key]} AND "
            query_string = query_string[:-5]

        # Get dataframe.
        return self.get_df(query_string)

class ConnectToTable(ConnectToDatabase):
    def __init__(self, db_file_path, table_name):
        """This creates an instance of a database connection with a specified table."""
        super().__init__(db_file_path)
        self.table_name = table_name

    def table_get_df(self, sql_condition):
        """Returns a dataframe corresponding to the result set of the SQL query string.
        Table name is specified to self.table_name."""
        query_string = f"SELECT * FROM {self.table_name} "
        query_string += f"WHERE {sql_condition}"
        dataframe = pandas.read_sql_query(query_string, self.conn)
        return dataframe
    
    def table_easy_get_df(self,  year=None, month=None, day=None, day_of_week=None, market_name=None, market_id=None, product_name=None, product_id=None, region=None, region_id=None, item_total=None, order_amount=None, high_price=None, mid_price=None, low_price=None, product_grade=None, product_class=None, brand=None, unit=None, overview=None, trend=None, sha256_hexdigest=None, ):
        return self.easy_get_df(table=self.table_name, year=year, month=month, day=day, day_of_week=day_of_week, market_name=market_name, market_id=market_id, product_name=product_name, product_id=product_id, region=region, region_id=region_id, item_total=item_total, order_amount=order_amount, high_price=high_price, mid_price=mid_price, low_price=low_price, product_grade=product_grade, product_class=product_class, brand=brand, unit=unit, overview=overview, trend=trend, sha256_hexdigest=sha256_hexdigest,) 



def make_shun_df(dataframe):
    list_of_df = []
    for year in range(2010, 2021):
        year_df = dataframe.query(f'年 == {year}')
        for month in range(1, 13):
            month_df = year_df.query(f'月 == {month}')
            joujun_df = month_df.query('日 < 10')
            chujun_df = month_df.query('日 >= 10 & 日 <= 20')
            gejun_df = month_df.query('日 > 20')

            joujun_amount_sum = joujun_df['数量'].sum()
            chujun_amount_sum = chujun_df['数量'].sum()
            gejun_amount_sum = gejun_df['数量'].sum()

            joujun_price_mean = joujun_df['価格'].mean()
            chujun_price_mean = chujun_df['価格'].mean()
            gejun_price_mean = gejun_df['価格'].mean()

            d = {
                "年": [year, year, year], 
                "月": [month, month, month], 
                "旬": ['jou', 'chu', 'ge'], 
                "都市名": dataframe.loc[0, '都市名'], 
                "都市コード": dataframe.loc[0, '都市コード'], 
                "品目名": dataframe.loc[0, '品目名'], 
                "品目コード": dataframe.loc[0, '品目コード'], 
                "産地名": dataframe.loc[0, '産地名'], 
                "産地コード": dataframe.loc[0, '産地コード'], 
                "総数量": [joujun_amount_sum, chujun_amount_sum, gejun_amount_sum], 
                "平均価格": [joujun_price_mean, chujun_price_mean, gejun_price_mean]            
            }
            list_of_df.append(pandas.DataFrame(data=d).fillna(0))        
    return_df = pandas.concat(list_of_df, ignore_index=True)
    return_df_copy = return_df.copy()
    return_df_copy['date_labels'] = (return_df['年'].astype('str') 
                                           + ' ' + return_df['月'].astype('str') 
                                           + ' ' + return_df['旬'])
    return return_df_copy
