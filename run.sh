#kafka-console-producer.sh --broker-list localhost:9092 --topic test

#spark-submit --class org.apache.spark.examples.streaming.NetworkWordCount ./netwc.jar localhost 9999

spark-submit --class org.apache.spark.examples.streaming.HwenTest ./test.jar localhost:9092 test /home/whwen/test/hello
