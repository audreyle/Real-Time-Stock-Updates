// Databricks notebook source
// MAGIC %md
// MAGIC #### Event Hub User Credentials

// COMMAND ----------

val topic = "ale314" 

val bootstrapServers = "bd520w2023-alt.servicebus.windows.net:9093"
val saslJaasConfig = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://bd520w2023-alt.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=lAJ6L1vOF3vT38JCLbJcd75DWC8itgLcy+AEhKyMoFM=\";"

// COMMAND ----------

import org.apache.spark.sql.functions._
 
val kafkaStream = spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", bootstrapServers) 
                 .option("kafka.security.protocol", "SASL_SSL")
                 .option("kafka.sasl.mechanism", "PLAIN")
                 .option("kafka.sasl.jaas.config", saslJaasConfig)
                 .option("subscribe", topic) //add offset?
                 .load()

// COMMAND ----------

display(kafkaStream)

// COMMAND ----------

val basicStream = kafkaStream.select($"key", $"timestamp", $"value")
display(basicStream)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Create Second Stream

// COMMAND ----------

val stream3 = basicStream.filter($"key"==="AAPL")
display(stream3)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Schematize JSON

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Second Stream Schema

// COMMAND ----------

val schema6 = new StructType()
    .add("c", DoubleType)
    .add("d", DoubleType)
    .add("dp", DoubleType)
    .add("h", DoubleType)
    .add("l", DoubleType)
    .add("o", DoubleType)
    .add("pc", DoubleType)
    .add("t", TimestampType)

// COMMAND ----------

val aaplQuoteStream = stream3.select($"key", 
                                    $"timestamp", 
                                    from_json($"value".cast(StringType),
                                    schema6).alias("appleDailySnapshot")
                                    )                                  

// COMMAND ----------

display(aaplQuoteStream)

// COMMAND ----------

// MAGIC %md
// MAGIC #####First Stream Schema

// COMMAND ----------

import org.apache.spark.sql.types._
val schema4 = new StructType()
    .add("data", ArrayType(new StructType()
        .add("c", ArrayType(StringType), true)
        .add("p", DoubleType)
        .add("s", StringType)
        .add("t", LongType)
        .add("v", IntegerType)))
    .add("type", StringType)

// COMMAND ----------


val lastActivityStream = kafkaStream.select($"key", 
                                           $"timestamp", 
                                           from_json($"value".cast(StringType),
                                           schema4).alias("lastPrices")
                                           )
lastActivityStream.printSchema

// COMMAND ----------

val dataToExplode = lastActivityStream.select($"timestamp", explode($"lastPrices.data").as("data"))

// COMMAND ----------

display(dataToExplode)

// COMMAND ----------

val stockStream = dataToExplode.select($"timestamp",
                                  $"data.s".as("ticker"),
                                  $"data.p".as("price"),
                                  $"data.v".as("volume"),
                                  $"data.t".as("tradeTime"),
                                  $"data.c".as("marketCondition"))

// COMMAND ----------

display(stockStream)

// COMMAND ----------

val lastPriceConverted = stockStream.select($"timestamp", $"ticker", $"price", $"volume", from_unixtime($"tradeTime"/1000, "yyyy-MM-dd HH:mm:ss").as("tradeTimeString"), $"marketCondition")

// COMMAND ----------

val lastPriceTimestamped = lastPriceConverted.select($"timestamp", $"ticker", $"price", $"volume", to_timestamp($"tradeTimeString").as("tradeTimestamp"), $"marketCondition")

// COMMAND ----------

display(lastPriceTimestamped)

// COMMAND ----------

lastPriceTimestamped.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ###### Counts, Averages -- Filters by Ticker

// COMMAND ----------

// Event window guarantee timeliness of AAPL's last prices
// Average last price for AAPL for messages that arrived within 2 minutes. Okay for data to overlap by 1 minute. 
val AAPLStream = lastPriceTimestamped.filter($"ticker".startsWith("AAPL")).groupBy($"ticker", $"price", $"volume", window($"timestamp", "2 minutes", "1 minute"))

// COMMAND ----------

val AAPLAvgPriceStream = AAPLStream.agg((avg($"price")).as("AAPLAveragePrice"))

// COMMAND ----------

display(AAPLAvgPriceStream)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Event Window & Watermarks

// COMMAND ----------

// Count how many last price records were received for all 4 stocks within a 2-minute window, okay to overlap by 1 minute.
val counts = lastPriceTimestamped.groupBy($"ticker", $"price", $"volume", window($"timestamp", "2 minutes", "1 minute")).count()//

// COMMAND ----------

display(counts)

// COMMAND ----------

// MAGIC %md
// MAGIC #Joining the Streams

// COMMAND ----------

val aaplQuote = aaplQuoteStream.select($"key", $"timestamp".as("aaplTimestamp"),
                                  $"appleDailySnapshot.dp".as("aaplPercentChange"),
                                  $"appleDailySnapshot.h".as("aaplHighPrice"),
                                  $"appleDailySnapshot.l".as("aaplLowPrice"),
                                  $"appleDailySnapshot.o".as("aaplOpenPrice"),
                                  $"appleDailySnapshot.pc".as("applPreviousClosePrice"),
                                  $"appleDailySnapshot.t".as("aaplTradeTimestamp"))

// COMMAND ----------

display(aaplQuote)

// COMMAND ----------

// watermark to output null if there is no match on timestamp within 15 minutes of message arrival
val aaplQuoteWatermarked = aaplQuote.withWatermark("aaplTradeTimestamp", "15 minutes")

// COMMAND ----------

display(aaplQuoteWatermarked)

// COMMAND ----------

// watermark to output null if there is no match on timestamp within 2 minutes of message arrival
val aaplStreamWithWatermark = lastPriceTimestamped.filter($"ticker".startsWith("AAPL")).withWatermark("timestamp", "10 seconds")

// COMMAND ----------

display(aaplStreamWithWatermark)

// COMMAND ----------

val mergedAAPLStream = aaplStreamWithWatermark.join(aaplQuoteWatermarked, 
  expr("""
  ticker = key AND
  timestamp >= aaplTimestamp + interval 1 minute
  """))
mergedAAPLStream.printSchema

// COMMAND ----------

display(mergedAAPLStream)

// COMMAND ----------

val aaplWCount = aaplQuoteWatermarked.groupBy("aaplTradeTimestamp").count().orderBy("aaplTradeTimestamp")
display(aaplWCount)

// COMMAND ----------

val aaplStreamWCount = aaplStreamWithWatermark.groupBy("tradeTimestamp").count().orderBy("TradeTimestamp")

// COMMAND ----------

display(aaplStreamWCount)

// COMMAND ----------

// MAGIC %md
// MAGIC ####Create a Delta Lake Sink

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Save checkpoints

// COMMAND ----------

import org.apache.spark.sql.streaming._

val checkPointDir = "dbfs:/ale314/checkpt"

// COMMAND ----------

lastActivityStream.isStreaming

// COMMAND ----------

// we're appending the updated aggregates
val query1 = lastActivityStream.writeStream.format("delta").outputMode("append").trigger(Trigger.ProcessingTime("1 second")).option("checkpointLocation", checkPointDir).table("stock_market_streaming_events") // Makes is a *managed* Delta

// COMMAND ----------

spark.table("stock_market_streaming_events").count()

// COMMAND ----------

val aaplCheckPointDir = "dbfs:/ale314/aaplcheckpt"

// COMMAND ----------

mergedAAPLStream.writeStream.format("delta").outputMode("append").trigger(Trigger.ProcessingTime("1 hour")).option("checkpointLocation", aaplCheckPointDir).table("aapl_streaming_events") // Makes is a *managed* Delta

// COMMAND ----------

spark.table("aapl_streaming_events").count()

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/ale314/"))

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/ale314/aaplcheckpt/offsets/"))

// COMMAND ----------

// MAGIC %sql
// MAGIC SHOW TABLES

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM stock_market_streaming_events

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM aapl_streaming_events

// COMMAND ----------

query1.lastProgress

// COMMAND ----------

query1.status

// COMMAND ----------

query1.stop()

// COMMAND ----------


