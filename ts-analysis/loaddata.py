import datetime
from pyspark.sql.types import *

def load_wiki_rdd(sc, path):
    def parse_line(line):
        tokens = line.split('\t')
        timestamp = datetime.datetime(int(tokens[0]), int(tokens[1]), int(tokens[2]), int(tokens[3]))
        page = tokens[4]
        views = int(tokens[5])
        return (timestamp, page, views)
    return sc.textFile(path).map(parse_line)

def load_wiki_df(sql_context, path):
    rdd = load_wiki_rdd(sql_context._sc, path)
    fields = [ \
        StructField('timestamp', TimestampType(), True), \
        StructField('page', StringType(), True), \
        StructField('views', IntegerType(), True)]
    schema = StructType(fields)
    return sql_context.createDataFrame(rdd, schema)

def load_ticker_rdd(sc, path):
    def parse_line(line):
        tokens = line.split('\t')
        timestamp = datetime.datetime(int(tokens[0]), int(tokens[1]), int(tokens[2]), int(tokens[3]))
        symbol = tokens[4]
        price = float(tokens[6])
        return (timestamp, symbol, price)

    return sc.textFile(path).map(parse_line)

def load_ticker_df(sqlContext, path):
    rdd = load_ticker_rdd(sqlContext._sc, path)
    fields = [ \
        StructField('timestamp', TimestampType(), True), \
        StructField('page', StringType(), True), \
        StructField('views', DoubleType(), True)]
    schema = StructType(fields)
    return sql_context.createDataFrame(rdd, schema)

