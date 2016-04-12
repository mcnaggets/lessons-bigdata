* hadoop fs -rm -r /test0003/out
* hadoop jar lessons-bigdata-1.0-SNAPSHOT.jar lesson.hadoop.Task3
* hadoop fs -cat /test0003/out/part-r-00000
* hadoop fs -text /test0003/out/part-r-00000.snappy
