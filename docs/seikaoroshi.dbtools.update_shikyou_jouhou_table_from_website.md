# seikaoroshi.dbtools.update_shikyou_jouhou_table_from_website

`dbtools.update_shikyou_jouhou_table_from_website`*(start_date, end_date, db_file_path, table_name="shikyou_jouhou")*

[青果物市況情報（日別）](https://www.seisen.maff.go.jp/transit/BS04B040UC010SC000.htm)で提供されているCSVデータをダウンロードし、各行にハッシュ値を加え、データベースに保存します。

## パラメータ

`start_date`

データの取得を開始する年月日を指定する datetime オブジェクト。

`end_date`

データの取得を終了する年月日を指定する datetime オブジェクト。ここで指定された前日までのデータが取得されます。

`db_file_path` 

データベースファイルを保存する場所を指定する path-like オブジェクト。

`table_name`

データベース内で保存するテーブルの名前。デフォルトは "shikyou_jouhou" 。
