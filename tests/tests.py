import unittest
from seikaoroshi import dbtools
from seikaoroshi import datatools


class TestAccessToMaffSite(unittest.TestCase):
    def test_table_instance(self):
        table_instance = dbtools.MarketTable("20200501")
        self.assertTrue(hasattr(table_instance, "table_rows"))

        row_instance = table_instance.get_table_row(1)
        self.assertEqual(row_instance.name, "盛岡市中央卸売市場")


class TestDownloadAndAccess(unittest.TestCase):
    def test_shikyou_jouhou_download(self):
        from datetime import datetime

        dbtools.update_shikyou_jouhou_table_from_website(
            start_date=datetime(2015, 11, 30),
            end_date=datetime(2015, 12, 1),
            db_file_path="unittest_database.db",
        )

        # Check for database consistency.
        conn = datatools.ConnectToDatabase("unittest_database.db")
        df = conn.get_df("SELECT * FROM shikyou_jouhou")
        row_0_hex = df.iloc[0]["sha256_hexdigest"]
        row_2873_hex = df.iloc[-1]["sha256_hexdigest"]
        self.assertEqual(len(df), 2874)
        self.assertEqual(
            row_0_hex,
            "e1a70f6869ae9d7427526d419bc5f119ae8de176b1d0fd5c8a511d52518f53b2",
        )
        self.assertEqual(
            row_2873_hex,
            "52f695be4cbb25b550ad587d1746cc6f22d29d3fef2b2d76404b61fdb3d4a03e",
        )

    def test_hibetsu_chousa_download(self):
        session_id = dbtools.get_hibetsu_session_id()
        dbtools.update_hibetsu_chousa_table_from_website(
            session_id=session_id,
            year=2017,
            month=11,
            tendays=3,
            db_file_path="unittest_database.db",
        )

        # Check for database consistency.
        conn = datatools.ConnectToDatabase("unittest_database.db")
        df = conn.get_df("SELECT * FROM hibetsu_chousa")
        row_0_hex = df.iloc[0]["sha256_hexdigest"]
        row_26976_hex = df.iloc[-1]["sha256_hexdigest"]
        self.assertEqual(len(df), 26977)
        self.assertEqual(
            row_0_hex,
            "8c1d74bddf8d0703edbab211a711c62a26eb5a31c39d924dc0643fa5771672c3",
        )
        self.assertEqual(
            row_26976_hex,
            "5b2f358baf1368a6b6cf30cfec6cbc64881c52260de5333dd667dac8a637beda",
        )

    # def tearDown(self):
    #     import os

    #     os.remove("unittest_database.db")


if __name__ == "__main__":
    unittest.main()
