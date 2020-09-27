# seikaoroshi.dbtools.update_hibetsu_chousa_table_from_website

`dbtools.update_hibetsu_chousa_table_from_website`*(session_id, year, month, tendays, db_file_path, table_name="hibetsu_chousa")*

[青果物卸売市場調査（日別調査）](https://www.seisen.maff.go.jp/transit/BS04B040UC020SC000.htm)のファイル一括ダウンロードで提供されているCSVデータをダウンロードし、各行にハッシュ値を加え、データベースに保存します。ダウンロードする期間の指定は年、月、旬（上旬、中旬、下旬）で指定します。

## パラメータ

`session_id`

[`dbtools.get_hibetsu_session_id()`](seikaoroshi.dbtools.get_hibetsu_session_id.md) で取得した、青果物卸売市場調査（日別調査）のサイトのセッションID。

`year`

ダウンロードする期間の年。

`month`

ダウンロードする期間の月。

`tendays`

ダウンロードする期間の旬。ひと月は上旬、中旬、下旬の3つの旬に分けられ、それぞれ数字の1、2、3が対応しています。つまり、ある月の中旬のデータをダウンロードしたい場合は `tendays=1` と指定します。


`db_file_path`

データベースファイルを保存する場所を指定する path-like オブジェクト。

`table_name`

データベース内で保存するテーブルの名前。デフォルトは "hibetsu_chousa" 。