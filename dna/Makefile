transaction_1_prod:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_transaction_1.py

transaction_1_dev:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_transaction_1.py --config configs/config-dev.yaml

transaction_2_prod:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_transaction_2.py

transaction_2_dev:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_transaction_2.py --config configs/config-dev.yaml

transaction_3_prod:
	spark-submit --driver-memory=15g --executor-memory=33g --exeuctor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_transaction_3.py

transaction_3_dev:
	spark-submit --driver-memory=15g --executor-memory=33g --exeuctor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_transaction_3.py --config configs/config-dev.yaml

coupon_and_digital_1_prod:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_coupon_and_digital_1.py

coupon_and_digital_1_dev:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_coupon_and_digital_1.py --config configs/config-dev.yaml

coupon_and_digital_2_prod:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_coupon_and_digital_2.py

coupon_and_digital_2_dev:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_coupon_and_digital_2.py --config configs/config-dev.yaml

member_prod:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_member.py

member_dev:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_member.py --configs configs/config-dev.yaml

acquisition_prod:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/integrate_acquisition.py

acquisition_dev:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/integrate_acquisition.py --config configs/config-dev.yaml

most_shopped_prod:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_most_shopped.py

most_shopped_dev:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_most_shopped.py -config configs/config-dev.yaml

misc_prod:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_misc.py

misc_dev:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/generate_misc.py --config configs/config-dev.yaml

transaction_1_and_2_prod:
	make transaction_1_prod && make transaction_2_prod

transaction_1_and_2_dev:
	make transaction_1_dev && make transaction_2_dev

member_and_acquisition_prod:
	make member_prod && make acquisition_prod

member_and_acquisition_dev:
	make member_dev && make acquisition_dev

feature_set_1_prod:
	make transaction_1_and_2_prod & \
	make member_and_acquisition_prod & \
	wait

feature_set_1_dev:
	make transaction_1_and_2_dev & \
	make member_and_acquisition_dev & \
	wait

feature_set_2_prod:
	make feature_set_1_prod && make misc_prod

feature_set_2_dev:
	make feature_set_1_dev && make misc_dev

coupon_and_digital_prod:
	make coupon_and_digital_1_prod && make coupon_and_digital_2_prod

coupon_and_digital_dev:
	make coupon_and_digital_1_dev && make coupon_and_digital_2_dev

all_features_prod:
	make feature_set_2_prod & \
	make coupon_and_digital_prod & \
	make transaction_3_prod & \
	make most_shopped_prod & \
	wait

all_features_dev:
	make feature_set_2_dev & \
	make coupon_and_digital_dev & \
	make transaction_3_dev & \
	make most_shopped_dev & \
	wait

merge_prod:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/merge.py

merge_dev:
	spark-submit --driver-memory=15g --executor-memory=33g --executor-cores=5 --conf spark.sql.shufflepartitions=800 scripts/merge.py --config configs/config-dev.yaml

run_member_dna_prod:
	make all_features_prod && \
	make merge_prod

run_member_dna_dev:
	make all_features_dev && \
	make merge_dev

# TODO: move member_dna to regression tests
unit_tests:
	@echo " *** Running Member DNA unit tests..."
	@spark-submit ${SPARK_UNITTEST_OPTIONS} tests/unit/dna_validation_unit.py & \
	spark-submit ${SPARK_UNITTEST_OPTIONS} tests/unit/coupon_digital_features_unit.py & \
	spark-submit --num-executors 3 --executor-cores 3 --executor-memory 10G --driver-memory 10G --conf spark.port.maxRetries=500 tests/unit/member_DNA_unit.py & \
	wait

tests: unit_tests
