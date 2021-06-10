from datetime import date
import operator
import pyorient


class IndexRateDay(object):
    def __init__(self, date=None, open=None, high=None, low=None, close=None, adj_close=None, volume=None, isin=None, region=None):
        self.date = date
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.adj_close = adj_close
        self.volume = volume
        self.isin = isin
        self.region = region


class StockDatabase(object):

    database = None

    def __init__(self, username, password, db_name, host, port, table_index_values_name):
        self.username = username
        self.password = password
        self.db_name = db_name
        self.table_index_values_name = table_index_values_name
        self.client = pyorient.OrientDB(host, port)

    def connect(self):
        self.client.connect(self.username, self.password)

    def recreate_tables(self):
        if self.client.db_exists(self.db_name):
            self.client.db_drop(self.db_name)

        self.client.db_create(
            self.db_name,
            pyorient.DB_TYPE_DOCUMENT,
            pyorient.STORAGE_TYPE_PLOCAL
        )

        self.database = self.client.db_open(
            self.db_name, self.username, self.password)
        self.client.command(
            "CREATE CLASS " + self.table_index_values_name + " IF NOT EXISTS")
        self.client.command("CREATE PROPERTY " +
                            self.table_index_values_name + ".Date DATE")
        self.client.command("CREATE PROPERTY " +
                            self.table_index_values_name + ".ISIN STRING")
        self.client.command("CREATE INDEX isinDate ON " +
                            self.table_index_values_name + " (ISIN, Date) UNIQUE")

    def query_index_values(self, isin, date_start=None, date_end=None, limit=-1):
        part_date_start = "" if date_start == None else ' AND Date >= "' + \
            self.__get_date_string(date_start) + '" '

        part_date_end = "" if date_end == None else ' AND Date < "' + \
            self.__get_date_string(date_end) + '" '

        query_string = 'SELECT FROM ' + self.table_index_values_name + ' WHERE ISIN = "' + \
            isin + '" ' + part_date_start + part_date_end + ' ORDER BY Date ASC'

        result = [x.oRecordData for x in self.client.query(
            query_string, limit)]
        result = list([IndexRateDay(x["Date"], x["Open"], x["High"], x["Low"], x["Close"],
                      x["Adj_Close"], x["Volume"], x["ISIN"], x["Region"]) for x in result])
        return sorted(result, key=operator.attrgetter('date'))

    def insert_index_values(self, df, isin, region):
        self.__prepare_index_data(df, isin, region)
        for index, row in df.iterrows():
            insert_command = "INSERT INTO " + \
                self.table_index_values_name + " CONTENT " + row.to_json()
            self.client.command(insert_command)

    def __prepare_index_data(self, df, isin, region):
        df.columns = df.columns.str.replace(' ', '_')
        df.dropna(inplace=True)
        df["ISIN"] = isin
        df["Region"] = region

    def __get_date_string(self, date):
        return date if isinstance(date, str) else date.strftime('%Y-%m-%d')
