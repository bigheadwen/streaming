#scalac -cp $KAFKA_HOME/libs/kafka_2.11-0.10.1.0.jar:$KAFKA_HOME/libs/jettison-1.1.jar @kk
rm -fr ./test.jar org;
scalac -cp $SPARK_HOME/lib/spark-assembly-1.6.1-hadoop2.6.0.jar:$SPARK_HOME/lib/spark-examples-1.6.1-hadoop2.6.0.jar @kk
jar cf test.jar org;
