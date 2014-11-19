###OOZIE Word Count MapReduce Program

This is a sample oozie job that will simply run the word count MR program. 

Now in order to use Oozie workflow you will have to create a particular folder structure on your machine

######Step 1 : Create folder structure  

* wordcount
..* job.properties
..* workflow.xml
..* lib/wordcount.jar


######Step 2 : Create job.properties

In the workcount folder create job.properties file like this, This file lets you pass parameters to your oozie workflow.Value of nameNode and jobTracker represent the name node and job tracker location. For this example I am using horton works sandbox. Please refer the file to see the enteries.

nameNode=hdfs://sandbox.hortonworks.com:8020
jobTracker=sandbox.hortonworks.com:8050
queueName=default
oozie.wf.application.path=${nameNode}/datasets/wordcount
outputDir=wordcount

######Step 3 : Create workflow
Next define your Apache oozie workflow.xml file like this. In my case the workflow has single step which is to execute mapreduce job.

* mapred.mapper.new-api & mapred.reducer.new-api: Set this property to true if your using the new MapReduce API based on org.apache.hadoop.mapreduce.* classes
* mapreduce.map.class: The fully qualified name of your mapper class
* mapreduce.reduce.class: The fully qualified name of your reducer class
* mapred.output.key.class: Fully qualified name of the output key class. This is same as parameter to job.setOutputKeyClass() in your driver class
* mapred.output.value.class: Fully qualified name of the output value class. This is same as parameter to job.setOutputValueClass() in your driver class
* mapred.input.dir: Location of your input file in my case i have wc.txt in hdfs://sandbox.hortonworks.com:8020/datasets directory
* mapred.output.dir:Location of output file that will get generated. 

Please refer the entries in the xml.

######Step 3 : Copy this folder to HDFS.

$ hadoop fs -put wordcount datasets/worcount.

######Step 4 : Run the oozie job from client location.

oozie job -oozie http://localhost:11000/oozie -config job.properties -run

You can debug and see the status of Job from the HUE oozie web interface
















