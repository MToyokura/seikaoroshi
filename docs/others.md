# その他

## データベースの設計
* 1つのデータベース（"seikabutsu_shikyou.db"）にデータが格納されたテーブル（"shikyou_jouhou"および"hibetsu_chousa"）が存在するという設計になっています。
* データベースへの保存は基本的に CSV → Pandas Dataframe → SQLite3 の順で行われます。
* 農林水産省のサイトからデータを取得する際、農林水産省が提供している CSV に "sha256_hexdigest" の列が自動で追加されます。これは各行を区別し、同じデータが重複して登録されることを防ぐためのハッシュ値です。
* 同一の日付で2度ダウンロードを行っても同一のデータが重複して登録されることはありません。
* 農林水産省のデータに修正があった場合の対応はできていません。データをダウンロードする場合は2週間前までのデータにしておくのが安全かもしれません。
* 現在確認できている、取得できるデータで最も古い日付は2010年3月です。


## Dependencies

<table>
<thead>
  <tr>
    <th>Package</th>
    <th>Minimum supported version</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td><a href="https://requests.readthedocs.io/en/master/" target="_blank">requests</a></td>
    <td>2.24.0</td>
  </tr>
  <tr>
    <td><a href="https://www.crummy.com/software/BeautifulSoup/bs4/doc/" target="_blank">BeautifulSoup</a></td>
    <td>4.9.0</td>
  </tr>
  <tr>
    <td><a href="https://pandas.pydata.org/" target="_blank">pandas</a></td>
    <td>1.1.2</td>
  </tr>
</tbody>
</table>