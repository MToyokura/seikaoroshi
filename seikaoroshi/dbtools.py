from datetime import datetime, timedelta
import re
import time
import os
from pathlib import Path
import hashlib
import sqlite3
import requests
from bs4 import BeautifulSoup
import pandas
import zipfile


# Stuff for making a database.
def tuple_to_string(the_tuple):
    """Converts tuple into one string."""
    list_of_tuple_elements = [str(i) + ", " for i in the_tuple]
    the_string = "".join(list_of_tuple_elements)
    return the_string


def list_to_string(the_list):
    """Converts list into one string."""
    strings_of_list_items = [str(i) + ", " for i in the_list]
    the_string = "".join(strings_of_list_items)
    return the_string


def get_sha256_of_string(the_string):
    """Returns SHA-256 hash for given string."""
    new_hash = hashlib.new("sha256")
    new_hash.update(bytes(the_string, "utf-8"))
    return new_hash


def get_sha256_hexdigest_of_tuple(the_tuple):
    """Returns a hexdigest string of SHA-256 hash of tuple."""
    the_string = tuple_to_string(the_tuple)
    the_hash = get_sha256_of_string(the_string)
    return the_hash.hexdigest()


def get_sha256_hexdigest_of_list(the_list):
    """Returns a hexdigest string of SHA-256 hash of list."""
    the_string = list_to_string(the_list)
    the_hash = get_sha256_of_string(the_string)
    return the_hash.hexdigest()


def return_dataframe_with_added_sha256_hash(dataframe):
    """Returns a dataframe with a column with sha256_hexdigest added."""
    # Create list of sha256 hexdigest.
    df_iter = dataframe.iterrows()
    hexdigest_list = []
    for row_num, pandas_series in df_iter:
        # df_iter = (row_num, pandas_series)
        row_items = pandas_series.items()
        # row_items = zip object (index_name, index_value)
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.items.html
        list_of_values = []
        for index, value in row_items:
            list_of_values.append(value)
            # list_of_values = [2011, 1, 5, 水, 盛岡, ...]
        hexdigest_list.append(get_sha256_hexdigest_of_list(list_of_values))
    # リスト内包表記でやろうとしたけどできなかった。。。
    # hexdigest_list = [value for index, value in [pandas_series.items() for pandas_series in [
    #     pandas_series for row_num, pandas_series in df_iter]]]

    # Turn list of hexdigests to pandas series, then add column to right end of dataframe.
    sha256_column = pandas.Series(hexdigest_list)
    new_dataframe = dataframe.assign(sha256_hexdigest=sha256_column)
    return new_dataframe


def add_sha256_to_csv(csv_file_path):
    """"Opens CSV as dataframe, then adds sha256 column, then saves CSV."""
    dataframe = pandas.read_csv(csv_file_path)
    new_dataframe = return_dataframe_with_added_sha256_hash(dataframe)
    new_dataframe.to_csv(csv_file_path, index=False)


def save_dataframe_to_db(dataframe, db_file_path, table_name):
    """Adds Dataframe to database table.

    Keyword arguments:
    dataframe -- a pandas Dataframe object
    db_file_path -- path for the database
    table_name -- name of table in database"""
    conn = sqlite3.connect(db_file_path)
    c = conn.cursor()

    table_names = c.execute("""SELECT name FROM sqlite_master WHERE type='table';""")
    table_names_list = [i for i in table_names]
    if table_name == "shikyou_jouhou":
        if not ("shikyou_jouhou",) in table_names_list:
            c.execute(
                """CREATE TABLE "shikyou_jouhou" (
            "年" INTEGER,
            "月" INTEGER,
            "日" INTEGER,
            "曜日" TEXT,
            "市場名" TEXT,
            "市場コード" INTEGER,
            "品目名" TEXT,
            "品目コード" INTEGER,
            "産地名" TEXT,
            "産地コード" REAL,
            "品目計" REAL,
            "入荷量" REAL,
            "高値" TEXT,
            "中値" TEXT,
            "安値" TEXT,
            "等級" TEXT,
            "階級" TEXT,
            "品名" TEXT,
            "量目" REAL,
            "概況" TEXT,
            "動向" TEXT,
            "sha256_hexdigest" TEXT,
            UNIQUE("sha256_hexdigest")
            )"""
            )  # Beware of trailing commas in SQL commands!!
            # A unique constraint is satisfied if and only if no two rows in a table have the same values and have non-null values in the unique columns.
            # https://sqlite.org/faq.html#q26
            # nullは異なるデータとして処理されるためUNIQUEとしては機能しない。
            # このため各行のデータをstringしたもののSHA-256を使ったハッシュ値を最後の列に追加しUNIQUEにする。
            # 産地コードが REAL なのは、pandas がそれをアサインしたからで、なぜそうなのかはわからない。
    elif table_name == "hibetsu_chousa":
        if not ("hibetsu_chousa",) in table_names_list:
            c.execute(
                """CREATE TABLE "hibetsu_chousa" (
                    "年" INTEGER,
                    "月" INTEGER,
                    "日" INTEGER,
                    "曜日" TEXT,
                    "都市名" TEXT,
                    "都市コード" INTEGER,
                    "品目名" TEXT,
                    "品目コード" INTEGER,
                    "産地名" TEXT,
                    "産地コード" REAL,
                    "数量" INTEGER,
                    "価格" INTEGER,
                    "対前日比（数量）" REAL,
                    "対前日比（価格）" REAL,
                    "sha256_hexdigest" TEXT,
                    UNIQUE("sha256_hexdigest")
                    )"""
            )

    dataframe = dataframe.drop_duplicates()
    # database sometimes contain duplicate rows, like those seen in "20110902札幌市中央卸売市場_fruits.csv" rows 27 and 30.
    dataframe.to_sql(table_name, con=conn, index=False, if_exists="append")


def save_csv_to_db(csv_file_path, db_file_path, table_name):
    """Reads csv files as dataframe, then adds sha256 hash, then appends to db.

    Keyword arguments:
    csv_file_path -- path of folder containing csv_files
    db_file_path -- path for the database
    table_name -- name of table in database
    """
    dataframe = pandas.read_csv(csv_file_path).drop_duplicates()
    # csv files sometimes contain duplicate rows, like those seen in "20110902札幌市中央卸売市場_fruits.csv" rows 27 and 30.
    new_dataframe = return_dataframe_with_added_sha256_hash(dataframe)
    save_dataframe_to_db(new_dataframe, db_file_path, table_name)
    print(f"Saved {csv_file_path} to\n Database: {db_file_path}\n Table:{table_name}")


# shikyou_jouhou database stuff
# Stuff for accessing the HTML of MAFF website.
class MarketTable:
    """This creates a Python object for the HTML table of given date.
    The date must be a 8 digit string, for example '20200428' .
    """

    def __init__(self, formatted_post_date):
        self.date = formatted_post_date
        # Request the table you see when you click a date.
        r = requests.post(
            "https://www.seisen.maff.go.jp/seisen/bs04b040md001/BS04B040UC010SC001-Evt001.do",
            data={"s006.dataDate": self.date},
        )
        if "公表済の帳票情報が存在しません" in r.text:
            print(f"data for {formatted_post_date} does not exist")
            time.sleep(1)
        else:
            self.soup = BeautifulSoup(r.text, "html.parser")
            self.table = self.soup.find_all(class_="scr1")[0]
            self.table_rows = self.table.find_all("tr")  # returns list of tr elements

    def get_table_row(self, int):
        """Returns a MarketTableRow instance of given row number of MarketTable instance."""
        return MarketTableRow(self.table_rows[int], self.date)


class MarketTableRow:
    """This creates a Python object for the given row of the HTML table .
    You can access market information as attributes and also perform downloads.
    Usually used via MarketTable.get_table_row().
    """

    def __init__(self, table_row_soup_object, date):
        self.date = date
        self.soup = table_row_soup_object
        self.id = self.soup.find_all("td")[0].get_text(strip=True)
        self.name = self.soup.find_all("td")[1].get_text(strip=True)

        if self.name == "市場名":
            raise Exception(
                "The HTML table row you tried to instantiate is probably the headings row."
            )
        # Get the 25 digit data id.
        self.veg_data_id = re.search(
            pattern=r"[0-9]{25}", string=str(self.soup.find_all("td")[2])
        ).group()
        self.fruit_data_id = re.search(
            pattern=r"[0-9]{25}", string=str(self.soup.find_all("td")[3])
        ).group()

    def get_veg_csv(self):
        """Sends request for vegetables csv data of HTML table row."""

        csv = requests.post(
            "https://www.seisen.maff.go.jp/seisen/bs04b040md001/BS04B040UC010SC001-Evt004.do",
            data={"s004.chohyoKanriNo": self.veg_data_id},
        )
        return csv

    def get_fruit_csv(self):
        """Sends request for fruits csv data of HTML table row."""
        csv = requests.post(
            "https://www.seisen.maff.go.jp/seisen/bs04b040md001/BS04B040UC010SC001-Evt004.do",
            data={"s004.chohyoKanriNo": self.fruit_data_id},
        )
        return csv

    def dl_veg_csv(self, folder_path=None):
        """Sends request for vegetables csv data of HTML table row, encodes it as UTF-8, then saves file to folder."""
        csv = self.get_veg_csv()
        with open(
            Path(folder_path, f"{self.date}{self.name}_vegetables.csv"),
            "w",
            encoding="UTF-8",
        ) as file:
            # Decode bytestring as SHIFT_JIS , then replace the \r\n to \n .
            file.write(csv.content.decode("cp932").replace("\r\n", "\n"))

    def dl_fruit_csv(self, folder_path=None):
        """Sends request for fruits csv data of HTML table row, encodes it as UTF-8, then saves file to folder."""
        csv = self.get_fruit_csv()
        with open(
            Path(folder_path, f"{self.date}{self.name}_fruits.csv"),
            "w",
            encoding="UTF-8",
        ) as file:
            # Decode bytestring as SHIFT_JIS , then replace the \r\n to \n .
            file.write(csv.content.decode("cp932").replace("\r\n", "\n"))


def update_shikyou_jouhou_table_from_website(
    start_date, end_date, db_file_path, table_name="shikyou_jouhou"
):
    """Downloads CSV to temp folder, then adds hash column, then saves to databse.

    Keyword arguments:
    start_date -- a datetime object for the start date
    end_date -- a datetime object for the end date
    db_file_path -- path for the database
    table_name -- name of table in database
    """
    get_date = start_date
    while get_date != end_date:
        # Get MarketTable instance of get_date.
        try:
            table_instance = MarketTable(get_date.strftime("%Y%m%d"))
        except requests.exceptions.ConnectionError:
            print("ConnectionError!!! Waiting 30 seconds to try again...")
            # Wait 30 seconds and connect again.
            time.sleep(30)
            table_instance = MarketTable(get_date.strftime("%Y%m%d"))
        except TimeoutError:
            print("TimeoutError!!! Waiting 30 minutes to try again...")
            # Wait 30 minutes and connect again.
            time.sleep(1800)
            table_instance = MarketTable(get_date.strftime("%Y%m%d"))
        except ConnectionResetError:
            print("ConnectionResetError!!! Waiting 30 minutes to try again...")
            # Wait 30 minutes and connect again.
            time.sleep(1800)
            table_instance = MarketTable(get_date.strftime("%Y%m%d"))

        # If MarketTable is successfully created, update database for each market.
        if hasattr(table_instance, "table_rows"):
            # Set path and make temp directories for csv.
            temp_folder_path = Path("shikyou_dl_temp")
            os.mkdir(temp_folder_path)
            try:
                # Save to database for each HTML table row.
                for i in range(1, len(table_instance.table_rows)):
                    row_instance = table_instance.get_table_row(i)

                    def dl_and_save():
                        # Download vegetable csv of HTML table row.
                        row_instance.dl_veg_csv(folder_path=temp_folder_path)
                        # Wait 1 second to prevent DOS
                        time.sleep(1)
                        # Download fruit csv of HTML table row.
                        row_instance.dl_fruit_csv(folder_path=temp_folder_path)
                        # Wait 1 second to prevent DOS
                        time.sleep(1)
                        # Save to database.
                        for csv_file in os.listdir(temp_folder_path):
                            try:
                                # Folder contains 2 files ('fruits' and 'vegetables')
                                save_csv_to_db(
                                    Path(temp_folder_path, csv_file),
                                    db_file_path,
                                    table_name,
                                )
                            except sqlite3.IntegrityError as e:
                                print(
                                    "IntegrityError for:\n"
                                    + str(Path(temp_folder_path, csv_file))
                                )
                                print(e.args)
                            finally:
                                # Delete csv
                                os.remove(Path(temp_folder_path, csv_file))

                    try:
                        dl_and_save()
                    except requests.exceptions.ConnectionError:
                        print("ConnectionError!!! Waiting 30 seconds to try again...")
                        # Wait 30 seconds and connect again.
                        time.sleep(30)
                        dl_and_save()
                    except TimeoutError:
                        print("TimeoutError!!! Waiting 30 minutes to try again...")
                        # Wait 30 minutes and connect again.
                        time.sleep(1800)
                        dl_and_save()
                    except ConnectionResetError:
                        print(
                            "ConnectionResetError!!! Waiting 30 minutes to try again..."
                        )
                        # Wait 30 minutes and connect again.
                        time.sleep(1800)
                        dl_and_save()
            finally:
                # Delete temp folder.
                os.rmdir(temp_folder_path)
        get_date += timedelta(days=1)


# hibetsu_chousa database stuff.


def get_hibetsu_session_id():
    """Access the hibetsu chousa website to retrieve the JSESSIONID cookie."""
    initial_access = requests.get(
        "https://www.seisen.maff.go.jp/seisen/bs04b040md001/BS04B040UC020SC998-Evt001.do"
    )
    for cookie in initial_access.cookies:
        if cookie.name == "JSESSIONID":
            session_id = cookie.value
            return session_id


def get_hibetsu_zipped_csv(session_id, year, month, tendays):
    """Downloads and returns zipped CSV file for given time.

    Keyword arguments:
    session_id -- JSESSIONID cookie value retrieved by get_hibetsu_session_id()
    year -- year of data that you want to download
    month -- month of data that you want to download
    tendays -- subdivision of month of data that you want to download. A year is divided into 3 parts.
    """
    year = str(year)
    month = str(month).zfill(2)
    tendays = str(tendays)

    payload = {
        "s027.sessionId": session_id,
        "s027.year": year,
        "s027.month": month,
        "s027.tendays": tendays,
        "s027.chohyoFileType": "CSV",
    }
    zipped_csv = requests.post(
        "https://www.seisen.maff.go.jp/seisen/bs04b040md001/BS04B040UC020SC002-Evt001.do",
        data=payload,
    )
    if zipped_csv.status_code == 200:
        return zipped_csv
    else:
        print(f"Status Code: {zipped_csv.status_code} for {year} {month} {tendays}")
        return None


def update_hibetsu_chousa_table_from_website(
    session_id, year, month, tendays, db_file_path, table_name="hibetsu_chousa"
):
    """Downloads zipped CSV to temp folder, then unzips file, then adds hash column, then saves to databse.

    Keyword arguments:
    session_id -- JSESSIONID cookie value retrieved by get_hibetsu_session_id()
    year -- year of data that you want to download
    month -- month of data that you want to download
    tendays -- subdivision of month of data that you want to download. A year is divided into 3 parts.
    db_file_path -- path for the database
    table_name -- name of table in database
    """
    zipped_csv = get_hibetsu_zipped_csv(session_id, year, month, tendays)
    # Save to temp folder
    temp_folder_path = Path("hibetsu_dl_temp")
    os.mkdir(temp_folder_path)
    temp_shift_jis_csv_files_folder_path = Path(temp_folder_path, "shift_jis_csv_files")
    os.mkdir(temp_shift_jis_csv_files_folder_path)
    temp_utf_8_csv_files_folder_path = Path(temp_folder_path, "shift_utf_8_files")
    os.mkdir(temp_utf_8_csv_files_folder_path)

    zip_file_path = Path(temp_folder_path, "zipped_csv.zip")
    with open(zip_file_path, "wb") as f:
        for chunk in zipped_csv.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(temp_shift_jis_csv_files_folder_path)
    os.remove(zip_file_path)

    for csv_file in os.listdir(temp_shift_jis_csv_files_folder_path):
        with open(
            Path(temp_shift_jis_csv_files_folder_path, csv_file),
            "r",
            encoding="shift_jis",
        ) as opened_file:
            content = opened_file.read()
            with open(
                Path(temp_utf_8_csv_files_folder_path, csv_file), "w", encoding="utf_8"
            ) as dest_file:
                dest_file.write(content)
        os.remove(Path(temp_shift_jis_csv_files_folder_path, csv_file))
    os.rmdir(temp_shift_jis_csv_files_folder_path)

    for csv_file in os.listdir(temp_utf_8_csv_files_folder_path):
        add_sha256_to_csv(Path(temp_utf_8_csv_files_folder_path, csv_file))
        try:
            save_csv_to_db(
                csv_file_path=Path(temp_utf_8_csv_files_folder_path, csv_file),
                db_file_path=db_file_path,
                table_name=table_name,
            )
        except sqlite3.IntegrityError as e:
            print("IntegrityError for:\n" + str(Path(temp_folder_path, csv_file)))
            print(e.args)
        finally:
            # Delete csv
            os.remove(Path(temp_utf_8_csv_files_folder_path, csv_file))
    os.rmdir(temp_utf_8_csv_files_folder_path)
    os.rmdir(temp_folder_path)
