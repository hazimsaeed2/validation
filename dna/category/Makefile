config_AH4_prod = prod/square_AH4.yaml
config_AH5_prod = prod/square_AH5.yaml
config_article_prod = prod/square_article.yaml
config_brand_prod = prod/square_brand.yaml

config_AH4_dev = dev/square_AH4.yaml
config_AH5_dev = dev/square_AH5.yaml
config_article_dev = dev/square_article.yaml
config_brand_dev = dev/square_brand.yaml

AH4_full_prod:
	spark-submit ${SPARK_OPTIONS} category_square/run.py --full-mode configs/$(config_AH4_prod) > AH4_full_prod.log 2>&1

AH5_full_prod:
	spark-submit ${SPARK_OPTIONS} category_square/run.py --full-mode configs/$(config_AH5_prod) > AH5_full_prod.log 2>&1

article_full_prod:
	spark-submit ${SPARK_OPTIONS} category_square/run.py --full-mode configs/$(config_article_prod) > article_full_prod.log 2>&1

brand_full_prod:
	spark-submit ${SPARK_OPTIONS} category_square/run.py --full-mode configs/$(config_brand_prod) > brand_full_prod.log 2>&1

run_all_full_prod:
	make AH4_full_prod & \
	make AH5_full_prod & \
	make article_full_prod & \
	make brand_full_prod & \
	wait

AH4_prod:
	spark-submit ${SPARK_OPTIONS} category_square/run.py --config configs/$(config_AH4_prod) > AH4_prod.log 2>&1

AH5_prod:
	spark-submit ${SPARK_OPTIONS} category_square/run.py --prod-mode configs/$(config_AH5_prod) > AH5_prod.log 2>&1

article_prod:
	spark-submit ${SPARK_OPTIONS} category_square/run.py --prod-mode configs/$(config_article_prod) > article_prod.log 2>&1

brand_prod:
	spark-submit ${SPARK_OPTIONS} category_square/run.py configs/$(config_brand_prod) > brand_prod.log 2>&1

run_all_prod:
	make AH4_prod & \
	make AH5_prod & \
	make article_prod & \
	make brand_prod & \
	wait

AH4_full_dev:
	spark-submit ${SPARK_OPTIONS} category_square/run.py --full-mode configs/$(config_AH4_dev) > AH4_full_dev.log 2>&1

AH5_full_dev:
	spark-submit ${SPARK_OPTIONS} category_square/run.py --full-mode configs/$(config_AH5_dev) > AH5_full_dev.log 2>&1

article_full_dev:
	spark-submit ${SPARK_OPTIONS} category_square/run.py --full-mode configs/$(config_article_dev) > article_full_dev.log 2>&1

brand_full_dev:
	spark-submit ${SPARK_OPTIONS} category_square/run.py --full-mode configs/$(config_brand_dev) > brand_full_dev.log 2>&1

run_all_full_dev:
	make AH4_full_dev & \
	make AH5_full_dev & \
	make article_full_dev & \
	make brand_full_dev & \
	wait

AH4_dev:
	spark-submit ${SPARK_OPTIONS} category_square/run.py --config configs/$(config_AH4_dev) > AH4_dev.log 2>&1

AH5_dev:
	spark-submit ${SPARK_OPTIONS} category_square/run.py --dev-mode configs/$(config_AH5_dev) > AH5_dev.log 2>&1

article_dev:
	spark-submit ${SPARK_OPTIONS} category_square/run.py --dev-mode configs/$(config_article_dev) > article_dev.log 2>&1

brand_dev:
	spark-submit ${SPARK_OPTIONS} category_square/run.py configs/$(config_brand_dev) > brand_dev.log 2>&1

run_all_dev:
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