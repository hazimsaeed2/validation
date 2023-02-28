unit_tests:
	@echo " *** Running common unit tests..."
	@spark-submit ${SPARK_OPTIONS} ${SPARK_UNITTEST_OPTIONS} tests/unit/check_import_statements_unit.py & \
	wait

tests: unit_tests