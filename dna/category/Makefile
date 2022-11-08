config_AH4_prod = prod/square_AH4.yaml
config_AH5_prod = prod/square_AH5.yaml
config_article_prod = prod/square_article.yaml
config_brand_prod = prod/square_brand.yaml
config_brand_exclusions_prod = prod/brand_excl_conf.yaml
reqs_prod = prod/reqs.yaml

config_AH4_stage = stage/square_AH4.yaml
config_AH5_stage = stage/square_AH5.yaml
config_article_stage = stage/square_article.yaml
config_brand_stage = stage/square_brand.yaml
config_brand_exclusions_stage = stage/brand_excl_conf.yaml
reqs_stage = stage/reqs.yaml

config_AH4_dev = dev/square_AH4.yaml
config_AH5_dev = dev/square_AH5.yaml
config_article_dev = dev/square_article.yaml
config_brand_dev = dev/square_brand.yaml
config_brand_exclusions_dev = dev/brand_excl_conf.yaml
reqs_dev = dev/reqs.yaml

brand_exclusions_prod:
	spark-submit ${SPARK_OPTIONS} scripts/brand_exclusions.py --config_path configs/$(config_brand_exclusions_prod) > brand_exclusions_prod.log 2>&1

AH4_full_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path configs/$(config_AH4_prod) --reqs_path configs/$(reqs_prod) > AH4_full_prod.log 2>&1

AH5_full_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path configs/$(config_AH5_prod) --reqs_path configs/$(reqs_prod) > AH5_full_prod.log 2>&1

article_full_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path configs/$(config_article_prod) --reqs_path configs/$(reqs_prod) > article_full_prod.log 2>&1

brand_full_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path configs/$(config_brand_prod) --reqs_path configs/$(reqs_prod) > brand_full_prod.log 2>&1

run_all_full_prod:
	make brand_exclusions_prod & \
	make AH4_full_prod & \
	make AH5_full_prod & \
	make article_full_prod & \
	make brand_full_prod & \
	wait

AH4_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --config_path configs/$(config_AH4_prod) --reqs_path configs/$(reqs_prod) > AH4_prod.log 2>&1

AH5_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --config_path configs/$(config_AH5_prod) --reqs_path configs/$(reqs_prod)> AH5_prod.log 2>&1

article_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --config_path configs/$(config_article_prod) --reqs_path configs/$(reqs_prod) > article_prod.log 2>&1

brand_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --config_path configs/$(config_brand_prod) --reqs_path configs/$(reqs_prod) > brand_prod.log 2>&1

run_all_prod:
	make brand_exclusions_prod & \
	make AH4_prod & \
	make AH5_prod & \
	make article_prod & \
	make brand_prod & \
	wait

brand_exclusions_stage:
	spark-submit ${SPARK_OPTIONS} scripts/brand_exclusions.py --config_path configs/$(config_brand_exclusions_stage) > brand_exclusions_stage.log 2>&1

AH4_full_stage:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path configs/$(config_AH4_stage) --reqs_path configs/$(reqs_stage) > AH4_full_stage.log 2>&1

AH5_full_stage:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path configs/$(config_AH5_stage) --reqs_path configs/$(reqs_stage) > AH5_full_stage.log 2>&1

article_full_stage:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path configs/$(config_article_stage) --reqs_path configs/$(reqs_stage) > article_full_stage.log 2>&1

brand_full_stage:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path configs/$(config_brand_stage) --reqs_path configs/$(reqs_stage) > brand_full_stage.log 2>&1

run_all_full_stage:
	make brand_exclusions_stage & \
	make AH4_full_stage & \
	make AH5_full_stage & \
	make article_full_stage & \
	make brand_full_stage & \
	wait

AH4_stage:
	spark-submit ${SPARK_OPTIONS} scripts/run.py--config_path configs/$(config_AH4_stage) --reqs_path configs/$(reqs_stage) > AH4_stage.log 2>&1

AH5_stage:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --config_path configs/$(config_AH5_stage) --reqs_path configs/$(reqs_stage) > AH5_stage.log 2>&1

article_stage:
	spark-submit ${SPARK_OPTIONS} scripts/run.py -config_path configs/$(config_article_stage) --reqs_path configs/$(reqs_stage) > article_stage.log 2>&1

brand_stage:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --config_path configs/$(config_brand_stage) --reqs_path configs/$(reqs_stage) > brand_stage.log 2>&1

run_all_stage:
	make brand_exclusions_stage & \
	make AH4_stage & \
	make AH5_stage & \
	make article_stage & \
	make brand_stage & \
	wait

brand_exclusions_dev:
	spark-submit ${SPARK_OPTIONS} scripts/brand_exclusions.py --config_path configs/$(config_brand_exclusions_dev) > brand_exclusions_dev.log 2>&1

AH4_full_dev:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path configs/$(config_AH4_dev) --reqs_path configs/$(reqs_dev) > AH4_full_dev.log 2>&1

AH5_full_dev:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path configs/$(config_AH5_dev) --reqs_path configs/$(reqs_dev) > AH5_full_dev.log 2>&1

article_full_dev:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path configs/$(config_article_dev) --reqs_path configs/$(reqs_dev) > article_full_dev.log 2>&1

brand_full_dev:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path configs/$(config_brand_dev) --reqs_path configs/$(reqs_dev) > brand_full_dev.log 2>&1

run_all_full_dev:
	make brand_exclusions_dev & \
	make AH4_full_dev & \
	make AH5_full_dev & \
	make article_full_dev & \
	make brand_full_dev & \
	wait

AH4_dev:
	spark-submit ${SPARK_OPTIONS} scripts/run.py--config_path configs/$(config_AH4_dev) --reqs_path configs/$(reqs_dev) > AH4_dev.log 2>&1

AH5_dev:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --config_path configs/$(config_AH5_dev) --reqs_path configs/$(reqs_dev) > AH5_dev.log 2>&1

article_dev:
	spark-submit ${SPARK_OPTIONS} scripts/run.py -config_path configs/$(config_article_dev) --reqs_path configs/$(reqs_dev) > article_dev.log 2>&1

brand_dev:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --config_path configs/$(config_brand_dev) --reqs_path configs/$(reqs_dev) > brand_dev.log 2>&1

run_all_dev:
	make brand_exclusions_dev & \
	make AH4_dev & \
	make AH5_dev & \
	make article_dev & \
	make brand_dev & \
	wait

unit_tests:
	@echo " *** Running Category DNA unit tests..."
	@spark-submit ${SPARK_UNITTEST_OPTIONS} tests/unit/category_DNA_unit.py & \
	wait

tests: unit_tests