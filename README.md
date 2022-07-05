# Network Intrusion Detection using Machine Learning in Big Data Environment

Major Project


## Instructions to setup

 1. Setup Apache Hadoop and Apache Spark on all the nodes.***All machines must have the same version of the OS java, scala, Apache Hadoop, Apache Spark and python***

 2. clone the repo to the master node.
`git clone https://github.com/sandeepSbudhya/Majorproject.git`
 3. create the virtual environment.
`pipenv shell`
 4. add the training CSV file to hadoop and set the path in the train script.
`hadoop fs -copyFromLocal /path/to/file/in/local/device /path/in/hadoop`
 5. run the train script.
`python3 train.py`
 6. run the test script. 
`python3 test.py`
