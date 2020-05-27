# Flink
1. Download Flink from https://flink.apache.org/downloads.html
```
$ cd ~/Downloads
$ tar xzf flink-*.tgz
$ cd flink-1.10.0
```
2. Then you can start cluster:
```
$ ./bin/start-cluster.sh
```
And that's it. You can visit http://localhost:8081 and show the result :)

3. To stop the cluser:
```
$ ./bin/stop-cluster.sh
```

4. If you want to test flink, you can run simple word count:
```
$ nc -l 9876
```
