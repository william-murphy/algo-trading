import sqlite3

class DatabaseManager:

    def __init__(self):
        self.connection = sqlite3.connect('data.db')

    def create_tables(self):
        cursor = self.connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS `master` (
                `id` INT AUTO_INCREMENT,
                `date` DATE(20),
                `ticker` VARCHAR(16),
                `type` VARCHAR(10),
                `value` FLOAT(20),
                PRIMARY KEY (`id`)
            );
        """)
        self.connection.commit()

    def drop_tables(self):
        cursor = self.connection.cursor()
        cursor.execute("""DROP TABLE IF EXISTS `master`;""")
        self.connection.commit()

    def close(self):
        self.connection.close()

    def get_active_tickers(self):
        cursor = self.connection.cursor()
        cursor.execute("""SELECT DISTINCT ticker FROM master;""")
        return cursor.fetchall()

    def insert_data(self, df):
        cursor = self.connection.cursor()
        cursor.executemany("""
            INSERT INTO `master` (date, ticker, type, value) VALUES (?, ?, ?, ?);
        """, df.values.tolist())
        self.connection.commit()
    