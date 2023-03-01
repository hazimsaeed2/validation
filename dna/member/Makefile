SPARK_OPTIONS?=
CONFIG_PATH?=./configs/config.yaml

transaction_1_prod:
	spark-submit ${SPARK_OPTIONS} scripts/generate_transaction_1.py --config_path ${CONFIG_PATH}

transaction_2_prod:
	spark-submit ${SPARK_OPTIONS} scripts/generate_transaction_2.py --config_path ${CONFIG_PATH}

transaction_3_prod:
	spark-submit ${SPARK_OPTIONS} scripts/generate_transaction_3.py --config_path ${CONFIG_PATH}

coupon_and_digital_1_prod:
	spark-submit ${SPARK_OPTIONS} scripts/generate_coupon_and_digital_1.py --config_path ${CONFIG_PATH}

coupon_and_digital_2_prod:
	spark-submit ${SPARK_OPTIONS} scripts/generate_coupon_and_digital_2.py --config_path ${CONFIG_PATH}

member_prod:
	spark-submit ${SPARK_OPTIONS} scripts/generate_member.py --config_path ${CONFIG_PATH}

acquisition_prod:
	spark-submit ${SPARK_OPTIONS} scripts/integrate_acquisition.py --config_path ${CONFIG_PATH}

most_shopped_prod:
	spark-submit ${SPARK_OPTIONS} scripts/generate_most_shopped.py --config_path ${CONFIG_PATH}

misc_prod:
	spark-submit ${SPARK_OPTIONS} scripts/generate_misc.py --config_path ${CONFIG_PATH}

transaction_1_and_2_prod:
	make transaction_1_prod && make transaction_2_prod

member_and_acquisition_prod:
	make member_prod && make acquisition_prod

feature_set_1_prod:
	make transaction_1_and_2_prod & \
	make member_and_acquisition_prod & \
	wait

feature_set_2_prod:
	make feature_set_1_prod && make misc_prod

coupon_and_digital_prod:
	make coupon_and_digital_1_prod && make coupon_and_digital_2_prod

all_features_prod:
	make feature_set_2_prod & \
	make coupon_and_digital_prod & \
	make transaction_3_prod & \
	make most_shopped_prod & \
	wait

merge_prod:
	spark-submit ${SPARK_OPTIONS} scripts/merge.py --config_path ${CONFIG_PATH}

run_member_dna_prod:
	make all_features_prod && \
	make merge_prod

# TODO: move member_dna to regression tests
unit_tests:
	@echo " *** Running Member DNA unit tests..."
	@spark-submit ${SPARK_UNITTEST_OPTIONS} tests/unit/dna_validation_unit.py & \
	spark-submit ${SPARK_UNITTEST_OPTIONS} tests/unit/coupon_digital_features_unit.py & \
	spark-submit --num-executors 3 --executor-cores 3 --executor-memory 10G --driver-memory 10G --conf spark.port.maxRetries=500 tests/unit/member_DNA_unit.py & \
	wait

tests: unit_tests
