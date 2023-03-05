SPARK_OPTIONS?=
CONFIG_PATH?=./configs/config.yaml

etl_complete_prod_step1_cluster1:
	spark-submit ${SPARK_OPTIONS} scripts/detail.py --config_path ${CONFIG_PATH}

etl_complete_prod_step1_cluster2:
	spark-submit ${SPARK_OPTIONS} scripts/item.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/brand.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/member.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/census_tract.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/awards.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/header.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/payment.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/fiscal_days.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/header_fiscal.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/awards_fiscal.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/member_history.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/skeleton.py --config_path ${CONFIG_PATH}

etl_complete_prod_step1_cluster3:
	spark-submit ${SPARK_OPTIONS} scripts/member_extended.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/email_mem.py --config_path ${CONFIG_PATH}

etl_complete_prod_step2_cluster1:
	spark-submit ${SPARK_OPTIONS} scripts/email_fiscal.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/item_add_brand.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/coupon_clip.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/coupon_clip_fiscal.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/quotient_id.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/payment_fiscal.py --config_path ${CONFIG_PATH}

etl_complete_prod_step2_cluster3:
	spark-submit ${SPARK_OPTIONS} scripts/detail_fiscal.py --config_path ${CONFIG_PATH}

etl_complete_prod_step3_cluster1:
	spark-submit ${SPARK_OPTIONS} scripts/club.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/club_square.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/detail_gas_nr_fiscal.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/control_files.py --config_path ${CONFIG_PATH}

etl_complete_prod_step3_cluster3:
	spark-submit ${SPARK_OPTIONS} scripts/detail_isnr_fiscal.py --config_path ${CONFIG_PATH}

post_etl_sampling:
	spark-submit ${SPARK_OPTIONS} scripts/sampling.py --config_path ${CONFIG_PATH} > etl_sampling.log 2>&1

post_etl_archiving:
	spark-submit ${SPARK_OPTIONS} scripts/archive.py --config_path ${CONFIG_PATH} > etl_archiving.log 2>&1

etl_complete:
	spark-submit ${SPARK_OPTIONS} scripts/item.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/brand.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/item_add_brand.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/header.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/detail.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/payment.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/fiscal_days.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/header_fiscal.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/detail_fiscal.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/detail_isnr_fiscal.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/detail_gas_nr_fiscal.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/payment_fiscal.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/member.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/member_extended.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/census_tract.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/member_history.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/skeleton.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/email_mem.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/email_fiscal.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/club.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/club_square.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/coupon_clip.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/coupon_clip_fiscal.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/awards.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/awards_fiscal.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/control_files.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/quotient_id.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/sampling.py --config_path ${CONFIG_PATH} && \
	spark-submit ${SPARK_OPTIONS} scripts/archive.py --config_path ${CONFIG_PATH}

unit_tests:
	@echo " *** Running ETL unit tests..."
	@spark-submit ${SPARK_OPTIONS} ${SPARK_UNITTEST_OPTIONS} tests/unit/etl_unit.py & \
	spark-submit ${SPARK_OPTIONS} ${SPARK_UNITTEST_OPTIONS} tests/unit/etl_validations_unit.py & \
	wait

tests: unit_tests
