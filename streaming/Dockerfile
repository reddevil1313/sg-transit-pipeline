FROM apache/spark:4.1.3-java21-python3

ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 $MAVEN/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar /opt/spark/jars/
ADD --chmod=644 $MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar /opt/spark/jars/
ADD --chmod=644 $MAVEN/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar /opt/spark/jars/
ADD --chmod=644 $MAVEN/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar /opt/spark/jars/

ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip
WORKDIR /app
COPY *.py .
