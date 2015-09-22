import datetime
from pyspark.sql.types import *
from pytz import utc

def local_datetime(utc_year, utc_month, utc_day, utc_hour):
    """This works around the fact that Spark SQL doesn't handle timezones"""
    dt = datetime.datetime(utc_year, utc_month, utc_day, utc_hour)
    epoch = datetime.datetime.utcfromtimestamp(0)
    seconds = (dt - epoch).total_seconds()
    return datetime.datetime.fromtimestamp(seconds)

def load_wiki_rdd(sc, path):
    """Creates an RDD of (timestamp, page, views).
    
    Parameters
    ----------
    sc - SparkContext to create the RDD in.
    path - Path to data.
    
    Returns
    -------
    An RDD of tuples (timestamp, page, views) of types (datetime.datetime, string, float)
    """
    def parse_line(line):
        tokens = line.split('\t')
        timestamp = local_datetime(int(tokens[0]), int(tokens[1]), int(tokens[2]), int(tokens[3]))
        page = tokens[4]
        views = float(tokens[5])
        return (timestamp, page, views)
    return sc.textFile(path).map(parse_line)

def load_wiki_df(sql_context, path):
    """Creates a Spark DataFrame of (timestamp, page, views).
    
    Parameters
    ----------
        sqlContext - SqlContext to create the RDD in.
        path - Path to data.
    
    Returns
    -------
        A Spark DataFrame with column names ('timestamp', 'page', 'views') of types (Timestamp, String, Double)
    """
    rdd = load_wiki_rdd(sql_context._sc, path)
    fields = [ \
        StructField('timestamp', TimestampType(), True), \
        StructField('page', StringType(), True), \
        StructField('views', DoubleType(), True)]
    schema = StructType(fields)
    return sql_context.createDataFrame(rdd, schema)

def load_ticker_rdd(sc, path):
    """Creates an RDD of (timestamp, symbol, price) for stock ticker data.
    
    Parameters
    ----------
        sc - SparkContext to create the RDD in.
        path - Path to data.
    
    Returns
    -------
        An RDD of tuples where ech tuple has the type (datetime.datetime, string, float).
    """
    def parse_line(line):
        tokens = line.split('\t')
        timestamp = local_datetime(int(tokens[0]), int(tokens[1]), int(tokens[2]), int(tokens[3]))
        symbol = tokens[4]
        price = float(tokens[6])
        return (timestamp, symbol, price)
    return sc.textFile(path).map(parse_line)

def load_ticker_df(sql_context, path):
    """Creates a Spark DataFrame of (timestamp, symbol, price).
    
    Parameters
    ----------
        sqlContext - SqlContext to create the RDD in.
        path - Path to data.
    
    Returns
    -------
        A Spark DataFrame with column names (timestamp, symbol, price) of types (Timestamp, String, Double)
    """
    rdd = load_ticker_rdd(sql_context._sc, path)
    fields = [ \
        StructField('timestamp', TimestampType(), True), \
        StructField('symbol', StringType(), True), \
        StructField('price', DoubleType(), True)]
    schema = StructType(fields)
    return sql_context.createDataFrame(rdd, schema)

