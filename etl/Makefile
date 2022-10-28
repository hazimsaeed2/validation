source_etl_complete_prod:
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

source_etl_complete_dev:
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

source_etl_complete_stage:
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
	@spark-submit ${SPARK_OPTIONS} ${SPARK_UNITTEST_OPTIONS} tests/unit/source_etl_unit.py & \
	spark-submit ${SPARK_OPTIONS} ${SPARK_UNITTEST_OPTIONS} tests/unit/source_etl_validations_unit.py & \
	wait

tests: unit_tests
