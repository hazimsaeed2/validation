unit_tests:
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.port.maxRetries=500 testing_support/lib/check_import_statements_unit.py & \
	make -C source_etl unit_tests & \
	make -C dna unit_tests & \
	make -C category_square unit_tests & \
	make -C pipelines -C assignment unit_tests & \
	make -C measurement unit_tests & \
	make -C pipelines -C lib unit_tests & \
	make -C scripts & \
	make -C lib & \
	wait

regression_tests:
	make -C pipelines -C assignment regression_test  & \
	wait
