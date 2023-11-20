import unittest
import sqlite3
from mylib.query import query1, query2, query3, query4

class TestMain(unittest.TestCase):

    def setUp(self):
        # Setting up a test database
        self.conn = sqlite3.connect("GroceryDB_test.db")
        # Assuming you have a table creation query, you would execute it here
        # e.g. self.conn.execute(CREATE TABLE ...)
        self.conn.close()

    def tearDown(self):
        # Cleaning up the test database after tests
        self.conn = sqlite3.connect("GroceryDB_test.db")
        self.conn.close()

    def test_query1(self):
        result = query1()
        self.assertEqual(result, "Success", "Failed to fetch the top 5 rows")

    def test_query2(self):
        result = query2()
        self.assertEqual(result, "Update Success",
                          "Failed to update the count_products of arabica coffee")

    def test_query3(self):
        result = query3()
        self.assertEqual(result, "Insert Success", "Failed to insert a new row")

    def test_query4(self):
        result = query4()
        self.assertEqual(result, "Delete Success",
                          "Failed to delete the row containing arabica coffee")

if __name__ == "__main__":
    unittest.main()
