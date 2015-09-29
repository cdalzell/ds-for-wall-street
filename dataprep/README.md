# How to Set Up The Cluster

Simply, install the latest CDH with Cloudera Manager!
http://www.cloudera.com/content/cloudera/en/products-and-services/cloudera-enterprise/cloudera-manager.html

The essential services are:

- HDFS
- YARN
- Zookeeper

Optional:

- Hue
- Spark (history server only)

But CDH does not include ipython, so...

# How to Set Up ipython

Note that you can install ipython locally without Hadoop, and perform most of the 
analysis in the tutorial locally on small data sets. On a cluster, however, the 
following need to be installed on all nodes in the cluster.

Note this will require Python 2.7 to be already present as well.

First, install key OS packages. For example, on Ubuntu:

```
sudo apt-get update
sudo apt-get install python-pip python-dev libgfortran3 gfortran libblas3gf libblas-doc libblas-dev liblapack3gf liblapack-doc liblapack-dev libfreetype6-dev python-matplotlib libxft-dev
```

Then install Python packages via `pip`:

```
sudo pip install --upgrade pip
sudo pip install --upgrade numpy scipy pandas jupyter matplotlib seaborn sklearn
```

Finally, to run ipython in front of Pyspark:

```
export PYSPARK_DRIVER_PYTHON=ipython
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.ip='*' --NotebookApp.open_browser=False --NotebookApp.port=8880"
pyspark --master yarn --deploy-mode client --driver-memory ... --num-executors  ... --executor-memory ... --executor-cores ...
```
