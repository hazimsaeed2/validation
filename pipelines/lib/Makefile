unit_tests:
	@echo " *** Running Source ETL unit tests..."
	@python3 tests/unit/iotools_unit.py & \
	spark-submit ${SPARK_OPTIONS} ${SPARK_UNITTEST_OPTIONS} tests/unit/spark_util_unit.py & \
	wait

tests: unit_tests
