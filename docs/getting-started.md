# 基本的な使い方

## インストール方法

pip を使ったインストールが可能です。

```
pip install git+https://github.com/MToyokura/seikaoroshi.git#egg=seikaoroshi
```


## 青果物市況情報（日別）データのダウンロード

[青果物市況情報（日別）](https://www.seisen.maff.go.jp/transit/BS04B040UC010SC000.htm)のデータをダウンロードする際の例を見てみます。

```python
from seikaoroshi import dbtools
from datetime import datetime

dbtools.update_shikyou_jouhou_table_from_website(
    start_date=datetime(2015, 11, 30),
    end_date=datetime(2015, 12, 1),
    db_file_path="my_sample_database.db",
)
```

これを実行すると、実行したディレクトリ内の `my_sample_database.db` というファイル（ディレクトリ内にすでに存在しない場合新たに作成されます）の、 `shikyou_jouhou` というテーブル内に2015年11月30日の青果物市況情報が記録されます。

ある期間のデータをまとめてダウンロードしたい場合は `start_date` と `end_date` を指定します。例えば、2015年11月30日から2016年1月31日までのデータをダウンロードしたい場合は下のような書き方になります。

```python
dbtools.update_shikyou_jouhou_table_from_website(
    start_date=datetime(2015, 11, 30),
    end_date=datetime(2016, 2, 1),
    db_file_path="my_sample_database.db",
)
```

詳しくは[APIの詳細](seikaoroshi.dbtools.update_shikyou_jouhou_table_from_website.md)をご覧ください。

## 青果物卸売市場調査（日別調査）データのダウンロード

[青果物卸売市場調査（日別調査）](https://www.seisen.maff.go.jp/transit/BS04B040UC020SC000.htm)のデータをダウンロードする際の例を見てみます。

青果物卸売市場調査（日別調査）のデータをダウンロードするためには一度`get_hibetsu_session_id`を使って、青果物卸売市場調査（日別調査）のサイトのセッションIDを取得する必要があります。

```python
from seikaoroshi import dbtools

session_id = dbtools.get_hibetsu_session_id()
```

セッションIDを取得した後、下のコードを実行します。

```python
dbtools.update_hibetsu_chousa_table_from_website(
    session_id=session_id,
    year=2017,
    month=11,
    tendays=3,
    db_file_path="my_sample_database.db",
)
```

これを実行すると、実行したディレクトリ内に `my_sample_database.db` というファイル（ディレクトリ内にすでに存在しない場合新たに作成されます）の、 `hibetsu_chousa` というテーブル内に2017年11月下旬の青果物市況情報が記録されます。

詳しくは[APIの詳細](seikaoroshi.dbtools.update_hibetsu_chousa_table_from_website.md)をご覧ください。

ある期間のデータをまとめてダウンロードしたい場合は手動で`for`ループを書く必要があります。例えば2012年のデータをダウンロードしたい場合は以下のような書き方になります。

```python
for year in range(2012, 2013):
    for month in range(1, 13):
        for tendays in range(1,4):
            dbtools.update_hibetsu_chousa_table_from_website(
                session_id=session_id,
                year=year,
                month=month,
                tendays=tendays,
                db_file_path="my_sample_database.db",
            )
```

## データベースファイルへのアクセス

`.db` ファイルにアクセスし、中身をPandasのDataFrameとして取得します。

まず `datatools.ConnectToDatabase()` を使いデータベースとのコネクションのインスタンスを作成します。次にコネクションのインスタンスのメソッドである `get_df()` を使い、DataFrameを取得します。 `get_df()` にはSQLクエリの文字列を引数として渡します。

```python
from seikaoroshi import datatools

conn = datatools.ConnectToDatabase('my_sample_database.db')
my_dataframe = conn.get_df("SELECT * FROM hibetsu_chousa")
```

