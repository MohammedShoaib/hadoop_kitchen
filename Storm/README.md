Apache Storm is a free and open source distributed realtime computation system. Storm makes it easy to reliably process unbounded streams of data, doing for realtime processing what Hadoop did for batch processing. Storm is simple, can be used with any programming language, and is a lot of fun to use!

### Storm Components
 
A Storm cluster follows a master-slave model where the master and slave processes are coordinated through ZooKeeper. The following are the components of a Storm cluster.

* Nimbus 
* Supervisor Nodes
* Zookeeper Cluster

#####Nimbus :

The Nimbus node is the master in a Storm cluster. It is responsible for distributing the application code across various worker nodes, assigning tasks to different machines, monitoring tasks for any failures, and restarting them as and when required. 

Nimbus is stateless and stores all of its data in ZooKeeper. There is a single Nimbus node in a Storm cluster. It is designed to be fail-fast, so when Nimbus dies, it can be restarted without having any effects on the already running tasks on the worker nodes. This is unlike Hadoop, where if the JobTracker dies, all the running jobs are left in an inconsistent state and need to be executed again.

####Supervisor Nodes :

Supervisor nodes are the worker nodes in a Storm cluster. Each supervisor node runs a supervisor daemon that is responsible for creating, starting, and stopping worker processes to execute the tasks assigned to that node. Like Nimbus, a supervisor daemon is also fail-fast and stores all of its state in ZooKeeper so that it can be restarted without any state loss. A single supervisor daemon normally handles multiple worker processes running on that machine.

####Zookeeper Cluster :

In any distributed application, various processes need to coordinate with each other and share some configuration information. ZooKeeper is an application that provides all these services in a reliable manner. Being a distributed application, Storm also uses a ZooKeeper cluster to coordinate various processes. All of the states associated with the cluster and the various tasks submitted to the Storm are stored in ZooKeeper. Nimbus and supervisor nodes do not communicate directly with each other but through ZooKeeper. As all data is stored in ZooKeeper, both Nimbus and the supervisor daemons can be killed abruptly without adversely affecting the cluster.

![image](https://cloud.githubusercontent.com/assets/5790297/6837263/13404fc0-d310-11e4-92c1-3592458208c1.png)

###Storm Installation : Single Node Cluster

$ sudo apt-get update

Prerequisites : 

In order to install storm we need following softwares installed..

	• Java (JDK 6 or above)

	• Python (2.6)

	• ZooKeeper
	
`$ sudo apt-get install openjdk-7-jdk`

```
$ java -version  
java version "1.7.0_75"
```

Python is built-in installed in ubuntu


Download Zookeeper and Storm:


`$ wget http://mirror.tcpdiag.net/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz`


`$ wget http://apache.mirrors.ionfish.org/storm/apache-storm-0.9.3/apache-storm-0.9.3.zip`

`$ sudo apt-get install unzip

$ tar xvfz zookeeper-3.4.6.tar.gz

$ unzip apache-storm-0.9.3.zip

$ sudo mv apache-storm-0.9.3/ /usr/lib/storm
$ sudo mv zookeeper-3.4.6/ /usr/lib/zookeeper`

Now all files are at /usr/lib/..  Now set the path in bash rc file..

`$ sudo vi .bashrc 
`

`export JAVA_HOME="/usr/lib/jvm/java-7-openjdk-amd64"    
export PATH="$PATH:$JAVA_HOME/bin"

export ZOOKEEPER_HOME="/usr/lib/zookeeper"
export PATH="$PATH:$ZOOKEEPER_HOME/bin"

export STORM_HOME="/usr/lib/storm"
export PATH="$PATH:$STORM_HOME/bin"`

$ source .bashrc 

Zookeeper Configuration :

$ touch /usr/lib/zookeeper/conf/zoo.cfg

Add following lines. (Ensure you created the directory)

tickTime=2000
dataDir=/home/ubuntu/tmp/zkeeper
clientPort=2181


$ zkServer.sh start
JMX enabled by default
Using config: /usr/lib/zookeeper/bin/../conf/zoo.cfg
Starting zookeeper ... STARTED

zkServer.sh status
JMX enabled by default
Using config: /usr/lib/zookeeper/bin/../conf/zoo.cfg
Mode: standalone


To interact with ZooKeeper from command line use the client..

$ zkCli.sh 

[zk: localhost:2181(CONNECTED) 1] ls /
[zookeeper]

Here its showing current znodes in zookeeper.

Configuration and starting Storm :

Edit storm.yaml and add following properties

$ vi /usr/lib/storm/conf/storm.yaml 

STORM Config :

yaml storm.zookeeper.servers:
     - "localhost"

yaml nimbus.host: "localhost"

yaml supervisor.slots.ports:
      - 6700
      - 6701
      - 6702
      - 6703

yaml storm.local.dir: "/home/ubuntu/tmp/storm"
 
$ storm nimbus 

Once started you will see storm znodes in zookeeper

$ storm supervisor

$ storm ui

ubuntu@ip-172-30-0-206:~$ jps
11279 core
10973 nimbus
10743 QuorumPeerMain
11138 supervisor
11329 Jps

To access webui : http://52.3.x.x:8080


