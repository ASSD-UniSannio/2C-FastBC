### If you need to restart Apache-Spark:
```
/home/unisannio/spark-2.2.0-bin-hadoop2.7/sbin/stop-all.sh
/home/unisannio/spark-2.2.0-bin-hadoop2.7/sbin/start-all.sh

IMPORTANT:  
WHEN YOU CHANGE THE SCALA REPOSITORY (proj3_fast_bc_weighted), PUSH THE CHANGES ON GIT WITH YOUR GIT CREDENTIALS. 
THEN, PULL THE NEW CHANGES ON THE SAME REPOSITORY ON THE LYON'S SERVER BY DOING A GIT PULL WITH YOUR GIT CREDENTIALS. 
```

### To run your sh script:
```
./scripts/knr_interpolated_rhone_alpes_script.sh
```

### List of parameters for executing W2C (and Brandes BC) with Spark:
```
HOME='/home/unisannio/proj3_fast_bc_weighted/'
```

##### EPSILON parameter of the louvain method: a new configuration is searched until currentQModularityValue > qModularityValue + epsilon
```
EPSILON=0
```

##### Where the output shall be written:
```
OUTPUT_FOLDER=$HOME'results/knr_interpolated_rhone_alpes/08_00_knr_interpolated_disc_data_est_travel_time_wc'
```

##### The input graph filepath:
```
INPUT_FILEPATH=$HOME'input/knr_interpolated_rhone_alpes/08_00_knr_interpolated_disc_data_est_travel_time_wc.csv'
```

##### IS_DIRECTED can be either 'd' (use it for directed graphs) or anything else (if you want the graph to be considered undirected)
```
IS_DIRECTED='d'
```

##### IS_WEIGHTED can be either 'w' (use it for for weighted graphs) or anything else (if you want the graph to be considered unweighted)
```
IS_WEIGHTED='w'
```

##### IS_WEIGHT_DIST specify the nature of the weight in the input graph:
If IS_WEIGHT_DIST equals to "dist" weights are considered in the sense "higher is worse" (e.g., when weights are spatial distances). 
Therefore they are inverted when applying the Louvain method and kept as they are when computing shortest paths for BC computation. 
Otherwise, weights are considered as "higher is better". 
Therefore, they are left as they are for Louvain method and inverted for shortest paths (e.g., when weights are "ranking"). 
```
IS_WEIGHT_DIST='dist'
```

##### The text separator to read the graph from the input file
```
SEPARATOR=','
```

##### You should not need to change these parameters (but you should do mvn install in the project when you change it on GitHub and you update the project on the server)
```
MASTER_URI='spark://TICIL1.ifsttar.fr:7077'
JAR_FILE=$HOME'target/fast_bc_weighted_graphs-0.0.1-SNAPSHOT-allinone.jar'
BRANDES_BC_CLASS='unisannio.BrandesBC'
FAST_BC_CLASS='unisannio.FastBC'
FAST_BC_2C_CLASS='unisannio.FastBC_2C'
TIMEOUT_INTERVAL='7200s'
MAX_RESULTS_SIZE='100g'
MAX_MSG_SIZE='999'
NUM_EXECUTORS=2	# IF YOU CHANGE THIS PARAMETER, REMEMBER TO CHANGE ACCORDINGLY THE SETTING -> export SPARK_WORKER_INSTANCES=2 in "/home/unisannio/spark-2.2.0-bin-hadoop2.7/conf/spark-env.sh"
```

##### The K-fraction parameters to test with the W2C-Fast-BC:
For each fraction in the list the W2C-Fast-BC will be executed with different K. So remove those you don't want to test
Higher fractions means higher accuracy and slower performance:
```
FRACT_CLASSES=(0.2) # 0.01 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1.0)
```

##### Number of iteration to use with K-means in W2C-FAST-BC:
```
MAX_ITERATIONS=100000000
```

##### Number of spark cores (i.e. threads) per executor. Spark has been configured to work with two executors (both on the same node)
```
NUM_CORES_PER_EXECUTOR=5
```

##### Maximum amount of memory to be allocated to the driver and executors (You have in total 128G. So MAX_DRIVER_MEMORY + NUM_EXECUTORS*MAX_EXECUTORS_MEMORY ~< 128G)
```
MAX_DRIVER_MEMORY='40g'
MAX_EXECUTORS_MEMORY='40g'
TOTAL_NUM_CORES=$(expr $NUM_CORES_PER_EXECUTOR \* $NUM_EXECUTORS)
```

### To execute 1C-Fast-BC version (old version):
```
echo '### Executing Fast-BC on '$INPUT_FILEPATH' with '$NUM_EXECUTORS' executors, '$MAX_EXECUTORS_MEMORY' max memory per executor, using '$TOTAL_NUM_CORES' cores in total.'
spark-submit \
		--class $FAST_BC_CLASS \
		--num-executors $NUM_EXECUTORS \
		--executor-cores $NUM_CORES_PER_EXECUTOR \
		--executor-memory $MAX_EXECUTORS_MEMORY \
		--driver-memory $MAX_DRIVER_MEMORY \
		--master $MASTER_URI \
		--total-executor-cores $TOTAL_NUM_CORES \
		--conf spark.driver.maxResultSize=$MAX_RESULTS_SIZE \
		--conf spark.driver.heartbeatinverval=$TIMEOUT_INTERVAL \
		--conf spark.executor.heartbeatinverval=$TIMEOUT_INTERVAL \
		--conf spark.network.timeout=$TIMEOUT_INTERVAL \
		$JAR_FILE \
		$MASTER_URI \
		$INPUT_FILEPATH $SEPARATOR $IS_DIRECTED $IS_WEIGHTED $TOTAL_NUM_CORES $EPSILON $OUTPUT_FOLDER'/'$TOTAL_NUM_CORES'_cores' &> output.log
mv output.log $OUTPUT_FOLDER'/'$TOTAL_NUM_CORES'_cores/basic'
```
### To execute Brandes exact version:
```
echo '### Executing Brandes-BC on '$INPUT_FILEPATH' with '$TOTAL_NUM_CORES' cores on each slave.'
spark-submit \
	--class $BRANDES_BC_CLASS \
	--num-executors $NUM_EXECUTORS \
	--executor-cores $NUM_CORES_PER_EXECUTOR \
	--executor-memory $MAX_EXECUTORS_MEMORY \
	--driver-memory $MAX_DRIVER_MEMORY \
	--master $MASTER_URI \
	--total-executor-cores $TOTAL_NUM_CORES \
	--conf spark.driver.maxResultSize=$MAX_RESULTS_SIZE \
	--conf spark.driver.heartbeatinverval=$TIMEOUT_INTERVAL \
	--conf spark.executor.heartbeatinverval=$TIMEOUT_INTERVAL \
	--conf spark.network.timeout=$TIMEOUT_INTERVAL \
	--conf spark.rpc.message.maxSize=$MAX_MSG_SIZE \
	$JAR_FILE \
	$MASTER_URI \
	$INPUT_FILEPATH $SEPARATOR $IS_DIRECTED $IS_WEIGHTED $IS_WEIGHT_DIST $OUTPUT_FOLDER'/'$TOTAL_NUM_CORES'_cores/exact/' &> output.log
mv output.log $OUTPUT_FOLDER'/'$TOTAL_NUM_CORES'_cores/exact'
echo '### Completed'
```

### To execute W2C-Fast-BC:
```
for C in ${FRACT_CLASSES[@]}
do
	echo '### Executing Fast-BC-2C on '$INPUT_FILEPATH' with '$TOTAL_NUM_CORES' cores on each slave and '$C' K-fraction.'
	spark-submit \
		--class $FAST_BC_2C_CLASS \
		--num-executors $NUM_EXECUTORS \
		--executor-cores $NUM_CORES_PER_EXECUTOR \
		--executor-memory $MAX_EXECUTORS_MEMORY \
		--driver-memory $MAX_DRIVER_MEMORY \
		--master $MASTER_URI \
		--total-executor-cores $TOTAL_NUM_CORES \
		--conf spark.driver.maxResultSize=$MAX_RESULTS_SIZE \
		--conf spark.driver.heartbeatinverval=$TIMEOUT_INTERVAL \
		--conf spark.executor.heartbeatinverval=$TIMEOUT_INTERVAL \
		--conf spark.network.timeout=$TIMEOUT_INTERVAL \
		--conf spark.rpc.message.maxSize=$MAX_MSG_SIZE \
		$JAR_FILE \
		$MASTER_URI \
		$INPUT_FILEPATH $SEPARATOR $IS_DIRECTED $IS_WEIGHTED $IS_WEIGHT_DIST $TOTAL_NUM_CORES $EPSILON $OUTPUT_FOLDER'/'$TOTAL_NUM_CORES'_cores' $C $MAX_ITERATIONS &> output.log
	mv output.log $OUTPUT_FOLDER'/'$TOTAL_NUM_CORES'_cores/dynamic_clustering_'$C'_class_fraction/'
	echo '### Completed'
done
```

### To evaluate the accuracy and the time performance of the algorithm with Python:
```
cd $HOME'python_code'
python retrieve_stats.py $OUTPUT_FOLDER'/'$TOTAL_NUM_CORES'_cores'
python plot_global_stats.py $OUTPUT_FOLDER'/'$TOTAL_NUM_CORES'_cores' $TOTAL_NUM_CORES
```

### To compute Brandes with Python:
```
python compute_non_norm_directed_weighted_bc_brandes.py $INPUT_FILEPATH $OUTPUT_FOLDER'/'$TOTAL_NUM_CORES'_cores/brandes_python/'
```

# More details on the algorithm used for clustering with Louvain. Used when you run the BC Scala classes.
# dga-graphx 

- GraphX Algorithms

The dga-graphX package contains several pre-built executable graph algorithms built on Spark using the GraphX framework.  

### pre-requisites

 * [Spark]  (http://spark.apache.org/)   0.9.0 or later
 * [graphX]  (http://spark.apache.org/docs/latest/graphx-programming-guide.html)   
 * [Gradle] (http://www.gradle.org/) 

### build

If necessary edit the build.gradle file to set your version of spark and graphX

> gradle clean dist

Check the build/dist folder for dga-graphx-0.1.jar.   


# Algorithms 

## Louvain

### about louvain

Louvain distributed community detection is a parallelized version of this work:
```
Fast unfolding of communities in large networks, 
Vincent D Blondel, Jean-Loup Guillaume, Renaud Lambiotte, Etienne Lefebvre, 
Journal of Statistical Mechanics: Theory and Experiment 2008 (10), P10008 (12pp)
```
In the original algorithm each vertex examines the communities of its neighbors and makes a chooses a new community based on a function to maximize the calculated change in modularity.  In the distributed version all vertices make this choice simultaneously rather than in serial order, updating the graph state after each change.  Because choices are made in parallel some choice will be incorrect and will not maximize modularity values, however after repeated iterations community choices become more stable and we get results that closely mirror the serial algorithm.

### running louvain

After building the package (See above) you can execute the lovain algorithm against an edge list using the provided script

```
bin/louvain

Usage: class com.soteradefense.dga.graphx.louvain.Main$ [options] [<property>=<value>....]

  -i <value> | --input <value>
        input file or path  Required.
  -o <value> | --output <value>
        output path Required
  -m <value> | --master <value>
        spark master, local[N] or spark://host:port default=local
  -h <value> | --sparkhome <value>
        SPARK_HOME Required to run on cluster
  -n <value> | --jobname <value>
        job name
  -p <value> | --parallelism <value>
        sets spark.default.parallelism and minSplits on the edge file. default=based on input partitions
  -x <value> | --minprogress <value>
        Number of vertices that must change communites for the algorithm to consider progress. default=2000
  -y <value> | --progresscounter <value>
        Number of times the algorithm can fail to make progress before exiting. default=1
  -d <value> | --edgedelimiter <value>
        specify input file edge delimiter. default=","
  -j <value> | --jars <value>
        comma seperated list of jars
  -z <value> | --ipaddress <value>
        Set to true to convert ipaddresses to Long ids. Defaults to false
  <property>=<value>....
```

To run a small local example execute:
```
bin/louvain -i examples/small_edges.tsv -o test_output --edgedelimiter "\t" 2> stderr.txt
```

Spark produces alot of output, so sending stderr to a log file is recommended.  Examine the test_output folder. you should see

```
test_output/
├── level_0_edges
│   ├── _SUCCESS
│   └── part-00000
├── level_0_vertices
│   ├── _SUCCESS
│   └── part-00000
└── qvalues
    ├── _SUCCESS
    └── part-00000
```

```
cat test_output/level_0_vertices/part-00000 
(7,{community:8,communitySigmaTot:13,internalWeight:0,nodeWeight:3})
(4,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:4})
(2,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:4})
(6,{community:8,communitySigmaTot:13,internalWeight:0,nodeWeight:4})
(8,{community:8,communitySigmaTot:13,internalWeight:0,nodeWeight:3})
(5,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:4})
(9,{community:8,communitySigmaTot:13,internalWeight:0,nodeWeight:3})
(3,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:4})
(1,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:5})

cat test_output/qvalues/part-00000 
(0,0.4134948096885813)
```

Note: the output is laid out as if you were in hdfs even when running local.  For each level you see an edges directory and a vertices directory.   The "level" refers to the number of times the graph has been "community compressed".  At level 1 all of the level 0 vertices in community X are represented by a single vertex with the VertexID: X.  For the small example all modulairyt was maximized with no community compression so only level 0 was computed.  The vertices show the state of each vertex while the edges file specify the graph structure.   The qvalues directory lists the modularity of the graph at each level of compression.  For this example you should be able to see all of vertices splitting off into two distinct communities (community 4 and 8 ) with a final qvalue of ~ 0.413


### running louvain on a cluster

To run on a cluster be sure your input and output paths are of the form "hdfs://<namenode>/path" and ensure you provide the --master and --sparkhome options.  The --jars option is already set by the louvain script itself and need not be applied.

### parallelism

To change the level of parallelism use the -p or --parallelism option.  If this option is not set parallelism will be based on the layout of the input data in HDFS.  The number of partitions of the input file sets the level of parallelism.   

### advanced

If you would like to include the louvain algorithm in your own compute pipeline or create a custom output format, etc you can easily do so by extending the com.soteradefense.dga.graphx.louvain.LouvainHarness class.  See HDFSLouvainRunner which extends LouvainHarness and is called by Main for the example above

