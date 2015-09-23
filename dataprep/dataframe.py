from pyspark.sql import SQLContext, Row

lines = sc.textFile("/user/admin/Wikipedia/*")
tokens = lines.map(lambda l: l.split("\t"))
data = tokens.map(lambda t: Row(year=int(t[0]), month=int(t[1]), day=int(t[2]), hour=int(t[3]), page=t[4], hits=int(t[5])))


sqlContext = SQLContext(sc)
wtDataFrame = sqlContext.createDataFrame(data)
wtDataFrame.registerTempTable("wt")

hitCountsRDD = sqlContext.sql("SELECT hits, COUNT(*) AS c FROM wt GROUP BY hits ORDER BY hits").cache()
hitCounts = hitCountsRDD.collect()
