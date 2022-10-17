source_etl_complete_prod:
	spark-submit --conf spark.sql.broadcastTimeout=-1 --executor-memory=80g --executor-cores=5 --conf spark.sql.shuffle.partitions=800 run.py

source_etl_complete_dev:
	spark-submit --conf --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.port.maxRetries=500 run.py --config configs/config-dev.yaml

unit_tests:
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.port.maxRetries=500 unittests/source_etl_unit.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.port.maxRetries=500 unittests/source_etl_validations_unit.py & \
	wait
