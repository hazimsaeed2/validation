unit_tests:
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.port.maxRetries=500 testing_support/lib/check_import_statements_unit.py & \
	make -C etl unit_tests & \
	make -C common & \
	make -C lib & \
	wait

regression_tests:
	make -C pipelines -C assignment regression_test  & \
	wait
