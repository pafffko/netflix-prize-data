#Kafka
1. Install Kafka: https://tecadmin.net/install-apache-kafka-ubuntu/
2. Start zookeeper server and kafka:
```
$ sudo systemctl start zookeeper
$ sudo systemctl start kafka
```
Basic operations:
 * create a topic:
 ```
$ cd /usr/local/kafka
$ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
```
* delete a topic:
 ```
$ ./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic 'giorgos-.*'
```
* list all available topics:
 ```
$ bin/kafka-topics.sh --list --zookeeper localhost:2181
```
