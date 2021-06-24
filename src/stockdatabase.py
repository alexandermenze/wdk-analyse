from datetime import date
import operator
import pyorient


class IndexRateDay(object):
    def __init__(self, date=None, open=None, close=None):
        self.date = date
        self.open = open
        self.close = close


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

    def query_index_values(self, isin, date_start=None, date_end=None, limit=-1):
        cluster_name = "index_" + isin

        part_date_start = "" if date_start == None else ' Date >= "' + \
            self.__get_date_string(date_start) + '" '

        part_date_end = "" if date_end == None else ' Date < "' + \
            self.__get_date_string(date_end) + '" '

        part_where = ""

        if part_date_start != "" and part_date_end != "":
            part_where = "WHERE " + part_date_start + " AND " + part_date_end
        elif part_date_start != "" and part_date_end == "":
            part_where = "WHERE " + part_date_start
        elif part_date_start == "" and part_date_end != "":
            part_where = "WHERE " + part_date_end

        query_string = 'SELECT FROM CLUSTER:' + cluster_name + \
            ' ' + part_where + ' ORDER BY Date ASC'

        result = [x.oRecordData for x in self.client.query(
            query_string, limit)]
        result = list([IndexRateDay(x["Date"], x["Open"], x["Close"])
                      for x in result])
        return sorted(result, key=operator.attrgetter('date'))

    def insert_index_values(self, df, isin):
        self.__prepare_index_data(df)

        cluster_name = "index_" + isin

        self.client.command(
            "ALTER CLASS " + self.table_index_values_name + " ADDCLUSTER " + cluster_name)

        for index, row in df.iterrows():
            insert_command = "INSERT INTO " + \
                self.table_index_values_name + " CLUSTER " + \
                cluster_name + " CONTENT " + row.to_json()
            self.client.command(insert_command)

    def __prepare_index_data(self, df):
        df.columns = df.columns.str.replace(' ', '_')

        if "Adj_Close" in df.columns:
            df["Close"] = df["Adj_Close"]

        df.drop(['High', 'Low', 'Adj_Close', 'Volume'],
                axis=1, errors='ignore', inplace=True)
        df.dropna(inplace=True)

    def __get_date_string(self, date):
        return date if isinstance(date, str) else date.strftime('%Y-%m-%d')
