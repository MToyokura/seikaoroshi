# seikaoroshi - 青果物卸売市場調査日別結果ダウンロード用パッケージ
seikaoroshi は、農林水産省が公開している[青果物市況情報（日別）](https://www.seisen.maff.go.jp/transit/BS04B040UC010SC000.htm)および[青果物卸売市場調査（日別調査）](https://www.seisen.maff.go.jp/transit/BS04B040UC020SC000.htm)からデータを取得し、分析するための Python パッケージです。

本パッケージは農林水産省のサイトのデータをダウンロードしてデータベースとして保存する `dbtools` と、保存したデータを参照・分析するための `datatools` に分かれています。

`dbtools` を使って取得したデータはSQLite3の `.db` ファイルとして保存されるため、本パッケージに含まれる`datatools` を使わずに、 R など他の言語やツールを使って分析することも可能です。

使い方は[こちら](https://mtoyokura.github.io/seikaoroshi)。