# netflix-prize-data

1. Download data:
```
$ mkdir <name_of_catalog>
$ cd <name_of_catalog>
$ wget http://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/movie_titles.csv
$ wget http://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/netflix-prize-data.zip
$ unzip netflix-prize-data.zip
```
2. Run Flink cluster:
```
$ flink-1.10.0/bin/start-cluster.sh
```
3. Run Consumer.java
4. Start broker:
```
$ systemctl start zookeeper
$ systemctl start kafka
```
To check if kafka is running:
```
$ systemctl status kafka
```
5. Run Kafka producer:
```
$ java -cp /usr/local/kafka/libs/*:KafkaProducer.jar \
com.example.bigdata.TestProducer copy-netflix-prize-data 15 kafka-netflix \
0 localhost:9092
```
