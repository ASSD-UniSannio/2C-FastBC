#!/bin/bash

# Connect to:
# ssh wesemantic-guest@156.54.166.83 -t screen -r -d 25780
# password is: !tunn3ll?

# You will automatically be logged on an SSH screen running on the Lyon server (the screen is tunneled to unisannio. So never close it!)

# On Lyon's server you are identified as user unisannio (password: Un1s4nn10L1c1t)
# YOU CAN BE ASKED FROM TIME TO TIME THIS PASSWORD (e.g., if you restart Spark)

# TO RESTART SPARK:
# /home/unisannio/spark-2.2.0-bin-hadoop2.7/sbin/stop-all.sh
# /home/unisannio/spark-2.2.0-bin-hadoop2.7/sbin/start-all.sh

# WHEN YOU CHANGE THE SCALA REPOSITORY (proj3_fast_bc_weighted), PUSH THE CHANGES ON GIT WITH YOUR GIT CREDENTIALS
# THEN, PULL THE NEW CHANGES ON THE SAME REPOSITORY ON THE LYON'S SERVER BY DOING A GIT PULL WITH YOUR GIT CREDENTIALS


# TO RUN THIS SH SCRIPT: from folder '/home/unisannio/proj3_fast_bc_weighted/' just do './scripts/knr_interpolated_rhone_alpes_script.sh'

HOME='/home/unisannio/proj3_fast_bc_weighted/'

# EPSILON parameter of the louvain method: a new configuration is searched until currentQModularityValue > qModularityValue + epsilon
EPSILON=0

# WHERE THE OUTPUT SHALL BE WRITTEN
OUTPUT_FOLDER=$HOME'results/knr_interpolated_rhone_alpes/08_00_knr_interpolated_disc_data_est_travel_time_wc'

# THE INPUT GRAPH FILEPATH
INPUT_FILEPATH=$HOME'input/knr_interpolated_rhone_alpes/08_00_knr_interpolated_disc_data_est_travel_time_wc.csv'

# IS_DIRECTED can be 'd' (for directed graphs) or anything else (the graph will be considered undirected)
IS_DIRECTED='d'

# IS_WEIGHTED can be 'w' (for weighted graphs) or anything else (the graph will be considered unweighted)
IS_WEIGHTED='w'

# IS_WEIGHT_DIST specify the nature of the weight in the input graph:
# If IS_WEIGHT_DIST equals to "dist" weights are considered in the sense "higher is worse" (e.g., when weights are spatial distances).
# Therefore they are inverted when applying the Louvain method and kept as they are when computing shortest paths for BC computation.
# Otherwise, weights are considered as "higher is better".
# Therefore, they are left as they are for Louvain method and inverted for shortest paths (e.g., when weights are "ranking")
IS_WEIGHT_DIST='dist'

# INPUT FILE SEPARATOR
SEPARATOR=','

# YOU SHOULD NOT CHANGE THE FOLLOWING PARAMETERS (but you should do mvn install in the project when you change it on GitHub and you update the project on the server)
# -------------
MASTER_URI='spark://TICIL1.ifsttar.fr:7077'
JAR_FILE=$HOME'target/fast_bc_weighted_graphs-0.0.1-SNAPSHOT-allinone.jar'
BRANDES_BC_CLASS='unisannio.BrandesBC'
FAST_BC_CLASS='unisannio.FastBC'
FAST_BC_2C_CLASS='unisannio.FastBC_2C'
TIMEOUT_INTERVAL='7200s'
MAX_RESULTS_SIZE='100g'
MAX_MSG_SIZE='999'
NUM_EXECUTORS=2	# IF YOU CHANGE THIS PARAMETER, REMEMBER TO CHANGE ACCORDINGLY THE SETTING -> export SPARK_WORKER_INSTANCES=2 in "/home/unisannio/spark-2.2.0-bin-hadoop2.7/conf/spark-env.sh"
# -------------

# SELECT THE FRACTION(S) FOR THE K PARAMETER YOU WANT TO USE WITH W2C-Fast-BC
# For each fraction in the list the W2C-Fast-BC will be executed with different K. So remove those you don't want to test
# Higher fractions means higher accuracy and slower performance. You may want to check just a few or more than the following list:
FRACT_CLASSES=(0.01 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1.0)

# NUMBER OF ITERATIONS TO BE USED FOR KMEANS IN W2C-FAST-BC
MAX_ITERATIONS=100000000

# NUMBER OF CORES (i.e. threads) PER EXECUTOR. Spark has been configured to work with two executors (both on the same node)
NUM_CORES_PER_EXECUTOR=5

# Maximum amount of memory allocated to the driver (You have in total 128G. So MAX_DRIVER_MEMORY + NUM_EXECUTORS*MAX_EXECUTORS_MEMORY ~< 128G)
MAX_DRIVER_MEMORY='40g'
MAX_EXECUTORS_MEMORY='40g'
TOTAL_NUM_CORES=$(expr $NUM_CORES_PER_EXECUTOR \* $NUM_EXECUTORS)

# THIS CODE EXECUTES 1C-Fast-BC version (old version)
# echo '### Executing Fast-BC on '$INPUT_FILEPATH' with '$NUM_EXECUTORS' executors, '$MAX_EXECUTORS_MEMORY' max memory per executor, using '$TOTAL_NUM_CORES' cores in total.'
# spark-submit \
#		--class $FAST_BC_CLASS \
#		--num-executors $NUM_EXECUTORS \
#		--executor-cores $NUM_CORES_PER_EXECUTOR \
#		--executor-memory $MAX_EXECUTORS_MEMORY \
#		--driver-memory $MAX_DRIVER_MEMORY \
#		--master $MASTER_URI \
#		--total-executor-cores $TOTAL_NUM_CORES \
#		--conf spark.driver.maxResultSize=$MAX_RESULTS_SIZE \
#		--conf spark.driver.heartbeatinverval=$TIMEOUT_INTERVAL \
#		--conf spark.executor.heartbeatinverval=$TIMEOUT_INTERVAL \
#		--conf spark.network.timeout=$TIMEOUT_INTERVAL \
#		$JAR_FILE \
#		$MASTER_URI \
#		$INPUT_FILEPATH $SEPARATOR $IS_DIRECTED $IS_WEIGHTED $TOTAL_NUM_CORES $EPSILON $OUTPUT_FOLDER'/'$TOTAL_NUM_CORES'_cores' &> output.log
# mv output.log $OUTPUT_FOLDER'/'$TOTAL_NUM_CORES'_cores/basic'

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

# TO EVALUATE YOUR RESULTS WITH THE PYTHON CODE
# cd $HOME'python_code'
# python retrieve_stats.py $OUTPUT_FOLDER'/'$TOTAL_NUM_CORES'_cores'
# python plot_global_stats.py $OUTPUT_FOLDER'/'$TOTAL_NUM_CORES'_cores' $TOTAL_NUM_CORES

# TO COMPUTE EXACT BRANDES BC WITH PYTHON:
# python compute_non_norm_directed_weighted_bc_brandes.py $INPUT_FILEPATH $OUTPUT_FOLDER'/'$TOTAL_NUM_CORES'_cores/brandes_python/'