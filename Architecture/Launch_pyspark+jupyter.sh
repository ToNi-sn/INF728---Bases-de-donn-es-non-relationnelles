#start master 
ubuntu@tp-hadoop-27:~/NoSQLProject/spark/spark-3.2.1-bin-hadoop3.2/sbin$ ./start-master.sh

#start workers on all machines
ubuntu@tp-hadoop-4:~/NoSQLProject/spark-3.1.2-bin-hadoop3.2/sbin$ ./start-worker.sh spark://192.168.3.235:7077

#ouverture des ports pour les UI pyspark
ssh -L  8085:localhost:8085 ubuntu@137.194.211.146 
ssh -L 8085:localhost:8080 ubuntu@tp-hadoop-27  

#lancement pyspark et jupyter notebook
./bin/pyspark --conf spark.cassandra.connection.host='192.168.3.207' --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 \
--master spark://192.168.3.235:7077 --driver-memory 4g --executor-memory 4g --num-executors 5 --executor-cores 4  --conf spark.dynamicAllocation.enabled=false

#ouverture du port pour l'UI jupyter
ssh -L 8888:localhost:8888 ubuntu@137.194.211.146 
ssh -L 8888:localhost:8888 ubuntu@tp-hadoop-31 
