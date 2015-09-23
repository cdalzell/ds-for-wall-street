/*
 * Copyright (C) 2015 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.datascience.dsforwallstreet.dataprep

import java.util.{Calendar,Locale,TimeZone}

import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.spark.SparkContext

object DataPrep {

  def wikipediaDataPref(sc: SparkContext) = {

    def pagecountsFileToRDD(path: Path) = {
      val mhdMatch = "pagecounts-(\\d{4})(\\d{2})(\\d{2})-(\\d{2})0000".r.findFirstMatchIn(path.getName).get
      val List(year, month, day, hour) = mhdMatch.subgroups.map(_.toInt)

      sc.textFile(path.toString).map { line =>
        val Array(site, page, hits, _) = line.split(" ")
        (site, page, hits.toInt)
      }.filter {
        case (site, _, hits) => site == "en" && hits >= 20
      }.map {
        case (_, page, hits) => (year, month, day, hour, page, hits)
      }
    }

    val paths = FileSystem.get(sc.hadoopConfiguration).listStatus(new Path("/user/.../Wikipedia")).map(_.getPath)
    val hourlyRDDs = paths.map(pagecountsFileToRDD)
    sc.union(hourlyRDDs).repartition(20).map(_.productIterator.mkString("\t")).saveAsTextFile("/user/admin/Wikipedia")

  }

  def tickerDataPref(sc: SparkContext) = {

    val tickerFiles = sc.wholeTextFiles("/.../")

    val ticks = tickerFiles.flatMap { case (file, csv) =>
      val ticker = file.split('/').last.split('.').head
      var baseTime = 0L
      csv.split("\n").map { line =>
        val Array(time, close, _, _, _, vol) = line.split(',')
        val tickTime =
          if (time.head == 'a') {
            baseTime = time.tail.toLong
            baseTime
          } else {
            baseTime + (180 * time.toLong)
          }
        (tickTime, ticker, close.toDouble, vol.toLong)
      }
    }


    val closeVolByHourTicker = ticks.map { case (tickTime, ticker, close, vol) =>
      val cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ENGLISH)
      cal.setTimeInMillis(tickTime * 1000)
      val year = cal.get(Calendar.YEAR)
      val month = cal.get(Calendar.MONTH) + 1
      val day = cal.get(Calendar.DAY_OF_MONTH)
      val hour = cal.get(Calendar.HOUR_OF_DAY)
      ((year, month, day, hour, ticker), (close, vol))
    }

    val tickerVolVWAP = closeVolByHourTicker.groupByKey().mapValues { closeVols =>
      val totalVol = closeVols.map { case (_, vol) => vol }.sum
      val vwap = closeVols.map { case (close, vol) => close * vol }.sum / totalVol
      (totalVol, vwap)
    }

    tickerVolVWAP.map { case ((y, m, d, h, ticker), (vol, vwap)) =>
        Seq(y, m, d, h, ticker, vol, vwap).mkString("\t")
    }.saveAsTextFile("/user/admin/Ticker/")

  }


}
