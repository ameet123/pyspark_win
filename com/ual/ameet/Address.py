from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

ADD = "C:\\Users\\ameet.chaubal\\Documents\\projects\\united\\address\\address\\address.csv"
ADD_HIST = "C:\\Users\\ameet.chaubal\\Documents\\projects\\united\\address\\address_hist\\address_hist.csv"
ADD_COLS = ["customer_id", "transaction_dtmz", "effective_dtmz", "foundry_insert_timestamp_millisec",
            "foundry_insert_timestamp_nanosec"]
ADDHIST_COLS = ["customer_id", "update_id", "active_dtmz", "inactive_dtmz", "effective_dtmz"]
ADD_FILTER_COL = "transaction_dtmz"
HIST_FILTER_COL = "active_dtmz"
HIST_FILTER_COL2 = "effective_dtmz"


class Address:
    def __init__(self):
        self.spark = SparkSession.builder.config("spark.ui.port", "34050").appName("AddressDebug").getOrCreate()
        self.sc = self.spark.sparkContext

        log4jLogger = self.sc._jvm.org.apache.log4j
        self.LOGGER = log4jLogger.LogManager.getLogger("Address")
        self.LOGGER.info("Starting App AddressDebug")
        self.addDF, self.addcnt = self._getDF(ADD)
        self.addHistDF, self.histcnt = self._getDF(ADD_HIST)
        self.LOGGER.info("History count:{}".format(self.histcnt))

    def _filter(self, df: DataFrame, column, start, end):
        # query = "{} BETWEEN CAST('{}' as TIMESTAMP) AND CAST('{}' as TIMESTAMP)".format(column, start, end)
        # return df.where(query)
        return df.filter(F.col(column).between(start, end))

    def _hist_filter(self, df: DataFrame, c1, s1, e1, c2, s2, e2):
        return df.filter(F.col(c1).between(s1, e1) & F.col(c2).between(s2, e2))

    def _getDF(self, file):
        addDF = self.spark.read.csv(file, header='true')

        count = addDF.count()
        return addDF, count

    def close(self):
        self.sc.stop()

    def hist_filter(self):
        addFilteredDF = self._hist_filter(self.addHistDF, HIST_FILTER_COL, "2019-06-27 00:00:00", "2019-06-28 00:00:00",
                                          HIST_FILTER_COL2, "2019-05-30 00:00:00", "2019-06-01 00:00:00")
        count = addFilteredDF.count()
        max_tx_dtmz = addFilteredDF.agg(F.max(F.col(HIST_FILTER_COL2))).collect()[0][0]
        min_tx_dtmz = addFilteredDF.agg(F.min(F.col(HIST_FILTER_COL2))).collect()[0][0]
        self.LOGGER.info(
            ">>AddressHistory Filtered. count={} max_dtmz={} min_dtmz={}".format(count, max_tx_dtmz, min_tx_dtmz))
        # SHow few records
        addFilteredDF.select(ADDHIST_COLS).show()

    def filter(self, df, filter_col):
        addFilteredDF = self._filter(df, filter_col, "2019-06-27 00:00:00", "2019-06-28 00:00:00")
        count = addFilteredDF.count()
        max_tx_dtmz = addFilteredDF.agg(F.max(F.col(filter_col))).collect()[0][0]
        min_tx_dtmz = addFilteredDF.agg(F.min(F.col(filter_col))).collect()[0][0]
        self.LOGGER.info(">>Address Filtered. count={} max_dtmz={} min_dtmz={}".format(count, max_tx_dtmz, min_tx_dtmz))
        # SHow few records
        addFilteredDF.select(ADD_COLS).show()

    def run(self):
        self.LOGGER.info(">>Add count={} Hist count={}".format(self.addcnt, self.histcnt))

    def showSchema(self, df):
        self.LOGGER.info("\n{}".format(df._jdf.schema().treeString()))


if __name__ == '__main__':
    add = Address()
    add.filter("transaction_dtmz", add.addDF)
    # add.hist_filter()
    add.close()
