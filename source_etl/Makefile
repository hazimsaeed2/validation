source_etl_complete_prod:
	spark-submit ${SPARK_OPTIONS} etl_tasks/item.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/brand.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/item_add_brand.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/header.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/detail.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/payment.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/fiscal_days.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/header_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/detail_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/detail_isnr_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/detail_gas_nr_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/payment_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/member.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/member_extended.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/census_tract.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/member_history.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/skeleton.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/email_mem.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/email_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/club.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/club_square.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/coupon_clip.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/coupon_clip_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/awards.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/awards_fiscal.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/control_files.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/quotient_id.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/sampling.py && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/archive.py

source_etl_complete_dev:
	spark-submit ${SPARK_OPTIONS} etl_tasks/item.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/brand.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/item_add_brand.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/header.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/detail.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/payment.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/fiscal_days.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/header_fiscal.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/detail_fiscal.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/detail_isnr_fiscal.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/detail_gas_nr_fiscal.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/payment_fiscal.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/member.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/member_extended.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/census_tract.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/member_history.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/skeleton.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/email_mem.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/email_fiscal.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/club.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/club_square.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/coupon_clip.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/coupon_clip_fiscal.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/awards.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/awards_fiscal.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/control_files.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/quotient_id.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/sampling.py --config configs/config-dev.yaml && \
	spark-submit ${SPARK_OPTIONS} etl_tasks/archive.py --config configs/config-dev.yaml

unit_tests:
	@echo " *** Running Source ETL unit tests..."
	@spark-submit ${SPARK_OPTIONS} ${SPARK_UNITTEST_OPTIONS} tests/unit/source_etl_unit.py & \
	spark-submit ${SPARK_OPTIONS} ${SPARK_UNITTEST_OPTIONS} tests/unit/source_etl_validations_unit.py & \
	wait

tests: unit_tests
