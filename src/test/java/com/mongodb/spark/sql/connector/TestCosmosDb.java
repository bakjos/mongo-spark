package com.mongodb.spark.sql.connector;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class TestCosmosDb {
  public static void main(String[] args) throws TimeoutException, StreamingQueryException {
    System.setProperty("hadoop.home.dir", "/tmp/spark");
    SparkSession session = SparkSession.builder()
        .master("local[*]")
        .appName("structuredViewingReport")
        .getOrCreate();

    session
        .readStream()
        .format("mongodb")
        .option(
            "spark.mongodb.connection.uri",
            "mongodb://rxu-staging:EeGsFkNBknm6tGCMXtoBhr8aMYOddyqFU9w5d9OamF3FH5zHV8vAAbCgfaZR55jT2DHdmXMbJKbL8juhavklSw==@rxu-staging.mongo.cosmos.azure.com:10255/rxu-rules?ssl=true&replicaSet=globaldb&retrywrites=false&appName=@rxu-staging@")
        .option("spark.mongodb.database", "rxu-rules")
        .option("spark.mongodb.collection", "rules_instance")
        .option("change.stream.startup.mode", "timestamp")
        .option("change.stream.startup.mode.timestamp.start.at.operation.time", "1698814800")
        .option("change.stream.lookup.full.document", "updateLookup")
        .option(
            "aggregation.pipeline",
            "[\n" + "  {\n"
                + "    \"$match\": {\n"
                + "      \"operationType\": { \"$in\": [\"insert\", \"update\", \"replace\"] }\n"
                + "    }\n"
                + "  }\n"
                + "]")
        .schema("_id STRING, \n" + "fullDocument STRING,\n" + "documentKey STRUCT<_id STRING>")
        .load()
        .writeStream()
        .trigger(Trigger.Continuous(1, TimeUnit.SECONDS))
        .option("checkpointLocation", "/Users/giovannygutierrez/tmp/mongo-spark/checkpoints")
        .foreach(new ForeachWriter<Row>() {
          @Override
          public void process(final Row value) {
            BsonDocument doc = BsonDocument.parse(value.getString(1));
            BsonValue _id = doc.get("_id");
            String h;
            if (_id.isObjectId()) {
              h = _id.asObjectId().getValue().toHexString();
            } else {
              h = _id.asString().getValue();
            }

            System.out.println("h: " + h);
          }

          @Override
          public void close(final Throwable errorOrNull) {}

          @Override
          public boolean open(final long partitionId, final long epochId) {
            return true;
          }
        })
        .start()
        .awaitTermination();
  }
}
