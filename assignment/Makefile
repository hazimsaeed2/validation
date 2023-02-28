.PHONY: list coupons assign assign_dev test

config = config.yml



list:
	@grep '^[^[:space:]].*:' Makefile

check_input:
	spark-submit ./scripts/check_input.py --conf ./conf/$(config)

coupons:
	spark-submit ./scripts/create_coupons.py --conf ./conf/$(config)

assign:
	spark-submit ./scripts/assign_offers.py --conf ./conf/$(config)

subset:
	spark-submit ./scripts/subsample_population.py --conf ./conf/$(config)

output:
	spark-submit ./scripts/generate_output_file.py --conf ./conf/$(config)

qc:
	spark-submit ./scripts/qc_assignments.py --conf ./conf/$(config)

sizing:
	spark-submit ./scripts/backtest_sizing.py --conf ./conf/$(config)

msmt_input:
	spark-submit ./scripts/generate_msmt_input.py --conf ./conf/$(config)

input_mail:
	spark-submit ./scripts/generate_input_mail.py --conf ./conf/$(config)

create_closure:
	spark-submit ./scripts/create_closure.py --conf ./conf/$(config)

assign_closure:
	spark-submit ./scripts/assign_closure.py --conf ./conf/$(config)

e2e_bbm:
	spark-submit ./scripts/check_input.py --conf $(config) > input_check.txt 2>&1
	spark-submit ./scripts/generate_input_mail.py --conf $(config) > generate_input_mail.txt 2>&1
	spark-submit ./scripts/create_coupons.py --conf $(config) > create_coupons.txt 2>&1
	spark-submit ./scripts/assign_offers.py --conf $(config) > assign_offers.txt 2>&1
	spark-submit ./scripts/backtest_sizing.py --conf $(config) > backtest_sizing.txt 2>&1
	spark-submit ./scripts/generate_output_file.py --conf $(config) > generate_output_file.txt 2>&1
	spark-submit ./scripts/qc_assignments.py --conf $(config) > qc_assignments.txt 2>&1

e2e_mmpc:
	spark-submit ./scripts/check_input.py --conf $(config) > input_check.txt 2>&1
	spark-submit ./scripts/generate_input_mail.py --conf $(config) > generate_input_mail.txt 2>&1
	spark-submit ./scripts/create_coupons.py --conf $(config) > create_coupons.txt 2>&1
	spark-submit ./scripts/create_closure.py --conf $(config) > create_closure.txt 2>&1
	spark-submit --conf spark.driver.memory=15g --conf spark.driver.maxResultSize=15g --conf spark.default.parallelism=256 --conf spark.sql.shuffle.partitions=256 --conf spark.executor.heartbeatInterval=20s ./scripts/assign_offers.py --conf $(config) > assign_offers.txt 2>&1
	spark-submit ./scripts/subsample_population.py --conf $(config) > subsample_population.txt 2>&1
	spark-submit ./scripts/assign_closure.py --conf $(config) > assign_closure.txt 2>&1
	spark-submit ./scripts/backtest_sizing.py --conf $(config) > backtest_sizing.txt 2>&1
	spark-submit ./scripts/generate_output_file.py --conf $(config) > generate_output_file.txt 2>&1
	spark-submit ./scripts/qc_assignments.py --conf $(config) > qc_assignments.txt 2>&1


finalize_assignment:
	spark-submit ./scripts/generate_msmt_input.py --conf $(config) > generate_msmt_intput.txt 2>&1
	spark-submit ./scripts/create_debug_files.py --conf $(config) > create_debug_files.txt 2>&1


regression_test:
	make _regression_test |& tee regression_test.txt
	cat regression_test.txt | grep TEST_OUTCOME | sort > regression_summary.txt 2>&1

_regression_test:
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/FailTest.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/HelloWorld.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/SingleCF.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/NoCoupon.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/AdjustedTrips.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/BBM.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/ForceBackIn.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/CouponUpdate.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/DummyCoupon.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/SubsampleDecile.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/LongitudinalRun.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/MultiConstructs.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/BacktoBack.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/BackToBackRun.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/Backfill.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/ReplaceWithNULL.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/LayoutVsUniversal.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/CustomizableBackfill.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/AssignClosures.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/CFBAUDynamicCategories.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/AnniversaryContent.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/AnniversaryDynamicContent.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/RewardsContent.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/AssignClosuresWithDedup.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/BackfillCouponReservation.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/CouponSorting.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/CouponMapping.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/ForceHoldout.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/StaticBauTargeting.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.sql.shuffle.partitions=3 --conf spark.port.maxRetries=500 --conf spark.sql.broadcastTimeout=-1 regression_tests/GenerateInputMail.py & \
	wait

unit_tests:
	python3 unittests/input_check_unit.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.port.maxRetries=500 unittests/assign_integration_unit.py & \
	spark-submit --num-executors 32 --executor-cores 1 --executor-memory 4G --driver-memory 4G --conf spark.port.maxRetries=500 unittests/assign_unit.py & \
	spark-submit --num-executors 32 --executor-cores 1 --executor-memory 4G --driver-memory 4G --conf spark.port.maxRetries=500 unittests/campaign_unit.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.port.maxRetries=500 unittests/coupon_unit.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.port.maxRetries=500 unittests/model_integration_unit.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.port.maxRetries=500 unittests/regression_unit.py & \
	spark-submit --num-executors 32 --executor-cores 1 --executor-memory 4G --driver-memory 4G --conf spark.port.maxRetries=500 unittests/backtest_sizing_unit.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.port.maxRetries=500 unittests/qc_unit.py & \
	spark-submit --num-executors 1 --executor-cores 1 --executor-memory 2G --driver-memory 2G --conf spark.port.maxRetries=500 unittests/create_closure_unit.py & \
	wait

test_performance:
	spark-submit ../../tests/regression/StressTest1.py --conf conf/test/StressTest1_config.yml