SPARK_OPTIONS?=
CONFIG_PATH?=./etl/configs/config.yaml

etl_complete_prod_step1_cluster1:
	spark-submit ${SPARK_OPTIONS} etl_tasks/detail.py

etl_complete_prod_step1_cluster2:
	spark-submit ${SPARK_OPTIONS} etl_tasks/item.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/brand.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/member.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/census_tract.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/awards.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/header.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/payment.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/fiscal_days.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/header_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/awards_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/member_history.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/skeleton.py

etl_complete_prod_step1_cluster3:
	spark-submit ${SPARK_OPTIONS} etl_tasks/member_extended.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/email_mem.py

etl_complete_prod_step2_cluster1:
	spark-submit ${SPARK_OPTIONS} etl_tasks/email_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/item_add_brand.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/coupon_clip.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/coupon_clip_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/quotient_id.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/payment_fiscal.py

etl_complete_prod_step2_cluster3:
	spark-submit ${SPARK_OPTIONS} etl_tasks/detail_fiscal.py

etl_complete_prod_step3_cluster1:
	spark-submit ${SPARK_OPTIONS} etl_tasks/club.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/club_square.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/detail_gas_nr_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/control_files.py

etl_complete_prod_step3_cluster3:
	spark-submit ${SPARK_OPTIONS} etl_tasks/detail_isnr_fiscal.py

post_etl_sampling:
	spark-submit ${SPARK_OPTIONS} etl_tasks/sampling.py > etl_sampling.log 2>&1

post_etl_archiving:
	spark-submit ${SPARK_OPTIONS} etl_tasks/archive.py > etl_archiving.log 2>&1

etl_complete_prod:
	spark-submit ${SPARK_OPTIONS} scripts/item.py && \
	spark-submit ${SPARK_OPTIONS} scripts/brand.py && \
	spark-submit ${SPARK_OPTIONS} scripts/item_add_brand.py && \
	spark-submit ${SPARK_OPTIONS} scripts/header.py && \
	spark-submit ${SPARK_OPTIONS} scripts/detail.py && \
	spark-submit ${SPARK_OPTIONS} scripts/payment.py && \
	spark-submit ${SPARK_OPTIONS} scripts/fiscal_days.py && \
	spark-submit ${SPARK_OPTIONS} scripts/header_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} scripts/detail_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} scripts/detail_isnr_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} scripts/detail_gas_nr_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} scripts/payment_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} scripts/member.py && \
	spark-submit ${SPARK_OPTIONS} scripts/member_extended.py && \
	spark-submit ${SPARK_OPTIONS} scripts/census_tract.py && \
	spark-submit ${SPARK_OPTIONS} scripts/member_history.py && \
	spark-submit ${SPARK_OPTIONS} scripts/skeleton.py && \
	spark-submit ${SPARK_OPTIONS} scripts/email_mem.py && \
	spark-submit ${SPARK_OPTIONS} scripts/email_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} scripts/club.py && \
	spark-submit ${SPARK_OPTIONS} scripts/club_square.py && \
	spark-submit ${SPARK_OPTIONS} scripts/coupon_clip.py && \
	spark-submit ${SPARK_OPTIONS} scripts/coupon_clip_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} scripts/awards.py && \
	spark-submit ${SPARK_OPTIONS} scripts/awards_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} scripts/control_files.py && \
	spark-submit ${SPARK_OPTIONS} scripts/quotient_id.py && \
	spark-submit ${SPARK_OPTIONS} scripts/sampling.py && \
	spark-submit ${SPARK_OPTIONS} scripts/archive.py

etl_complete_dev:
	spark-submit ${SPARK_OPTIONS} scripts/item.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/brand.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/item_add_brand.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/header.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/detail.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/payment.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/fiscal_days.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/header_fiscal.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/detail_fiscal.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/detail_isnr_fiscal.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/detail_gas_nr_fiscal.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/payment_fiscal.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/member.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/member_extended.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/census_tract.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/member_history.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/skeleton.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/email_mem.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/email_fiscal.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/club.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/club_square.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/coupon_clip.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/coupon_clip_fiscal.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/awards.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/awards_fiscal.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/control_files.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/quotient_id.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/sampling.py --config_path configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/archive.py --config_path configs/config-dev.yaml

etl_complete_stage:
	spark-submit ${SPARK_OPTIONS} scripts/item.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/brand.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/item_add_brand.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/header.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/detail.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/payment.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/fiscal_days.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/header_fiscal.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/detail_fiscal.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/detail_isnr_fiscal.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/detail_gas_nr_fiscal.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/payment_fiscal.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/member.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/member_extended.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/census_tract.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/member_history.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/skeleton.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/email_mem.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/email_fiscal.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/club.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/club_square.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/coupon_clip.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/coupon_clip_fiscal.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/awards.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/awards_fiscal.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/control_files.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/quotient_id.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/sampling.py --config_path configs/config-stage.yaml && \
	spark-submit ${SPARK_OPTIONS} scripts/archive.py --config_path configs/config-stage.yaml

unit_tests:
	@echo " *** Running ETL unit tests..."
	@spark-submit ${SPARK_OPTIONS} ${SPARK_UNITTEST_OPTIONS} tests/unit/etl_unit.py & \
	spark-submit ${SPARK_OPTIONS} ${SPARK_UNITTEST_OPTIONS} tests/unit/etl_validations_unit.py & \
	wait

tests: unit_tests
