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




