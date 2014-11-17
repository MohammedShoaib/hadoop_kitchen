hadoop_kitchen
==============

This is the git home for storing my Hadoop projects or sample snippets.

#### Hadoop Commands
---------------------

*hadoop fs -ls                     (list all files in users home directory)
*hadoop fs -cat datasets/test.txt  (open file in hdfs location)
*hadoop fs -mkdir datasets/hello   (creates a folder in datasets directory)
*hadoop fs -rm -r datasets/hello   (delete the folder hello and all its contents)
*hadoop fs -get datasets/test.txt  (copy test.txt to local)

##### Administration
--------------------

hadoop  namenode  -format          	(formats the namenode. all data in hdfs is lost)
start-dfs.sh                       			(start the hdfs daemons)
start-yarn.sh                      			(start yarn daemons)
mr-jobhistory-daemon.sh start historyserver (start job history server)
