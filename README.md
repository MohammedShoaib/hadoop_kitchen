hadoop_kitchen
==============

This is the git home for storing my Hadoop projects or sample snippets.

#### Hadoop Commands
---------------------

* hadoop fs -ls                     (list all files in users home directory)
* hadoop fs -cat datasets/test.txt  (open file in hdfs location)
* hadoop fs -mkdir datasets/hello   (creates a folder in datasets directory)
* hadoop fs -rm -r datasets/hello   (delete the folder hello and all its contents)
* hadoop fs -get datasets/test.txt  (copy test.txt to local)
* hadoop jar MRjobs/target/mrjobs-0.1.jar com.hadoopgeek.test.Test -D color=yellow |grep color (using generic option parser)
* $ hadoop jar Technical/git_home/hadoop_kitchen/MRjobs/target/mrjobs-0.1.jar com.hadoopgeek.weather.Weather -D mapreduce.job.reduces=2  datasets/ncdc datasets/ncdc/out (Parser option, ensure that Tool interface is implemented)
* hadoop fs -getmerge datasets/ncdc/out weath_results  (merge reduce results (part files) into one and save in localpath)


###### Administration

* hadoop  namenode  -format          	(formats the namenode. all data in hdfs is lost)
* start-dfs.sh                       			(start the hdfs daemons)
* start-yarn.sh                      			(start yarn daemons)
* mr-jobhistory-daemon.sh start historyserver (start job history server)
* hadoop fs -chown -R jithesh /user/jithesh  (set home folder in hdfs for jithesh. you have to create folder structure first.)

###### Hadoop versions and MapReduce.
There are two MapReduce Frameworks . (classic and yarn).  In the same way there are old and new MapReduce API's. It always confuses developers.

| Features               |  0.2 | 1.X | 2.X  |
|------------------------|------|-----|------|
| Old MR API's           | Y    | Y   | Y    |  
| NEW MR API's           | y    | y   | Y    |  
| MR Classic (Framework) | Y    | Y   | Y    |
| MR Yarn (Framework)    | N    | N   | Y    |

Its important to realize that the old and new MapReduce APIs are not the same thing as the classic and YARN-based MapReduce implemen- tations (MapReduce 1 and 2, respectively). The APIs are user-facing client-side features and determine how you write MapReduce programs, whereas the implementations are just different ways of running Map- Reduce programs. All four combinations are supported: both the old and new API run on both MapReduce 1 and 2.

In hadoop 2.X which framework to be used is decided by the entry  "mapreduce.framework.name" . It can be "local" , "classic" or "yarn". 














