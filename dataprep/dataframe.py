from pyspark.sql import SQLContext, Row

lines = sc.textFile("/user/admin/Wikipedia/*")
def parse_line(line):
    tokens = line.split('\t')
    return Row(page=tokens[4], hits=int(tokens[5]))
data = lines.map(parse_line)

sqlContext = SQLContext(sc)
wtDataFrame = sqlContext.createDataFrame(data)
wtDataFrame.registerTempTable("wt")

hitCountsRDD = sqlContext.sql("SELECT hits, COUNT(*) AS c FROM wt GROUP BY hits ORDER BY hits").cache()
hitCounts = hitCountsRDD.collect()
