config_AH4 = configs/square_AH4.yaml
config_AH5 = configs/square_AH5.yaml
config_article = configs/square_article.yaml
config_brand = configs/square_brand.yaml
config_brand_exclusions = bconfigs/rand_excl_conf.yaml
reqs = configs/reqs.yaml

brand_exclusions_prod:
	spark-submit ${SPARK_OPTIONS} scripts/brand_exclusions.py --config_path $(config_brand_exclusions) > brand_exclusions_prod.log 2>&1

AH4_full_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path $(config_AH4) --reqs_path $(reqs) > AH4_full_prod.log 2>&1

AH5_full_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path $(config_AH5) --reqs_path $(reqs) > AH5_full_prod.log 2>&1

article_full_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path $(config_article) --reqs_path $(reqs) > article_full_prod.log 2>&1

brand_full_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --full-mode --config_path $(config_brand) --reqs_path $(reqs) > brand_full_prod.log 2>&1

run_all_full_prod:
	make brand_exclusions_prod & \
	make AH4_full_prod & \
	make AH5_full_prod & \
	make article_full_prod & \
	make brand_full_prod & \
	wait

AH4_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --config_path $(config_AH4) --reqs_path $(reqs) > AH4_prod.log 2>&1

AH5_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --config_path $(config_AH5) --reqs_path $(reqs)> AH5_prod.log 2>&1

article_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --config_path $(config_article) --reqs_path $(reqs) > article_prod.log 2>&1

brand_prod:
	spark-submit ${SPARK_OPTIONS} scripts/run.py --config_path $(config_brand) --reqs_path $(reqs) > brand_prod.log 2>&1

run_all_prod:
	make brand_exclusions_prod & \
	make AH4_prod & \
	make AH5_prod & \
	make article_prod & \
	make brand_prod & \
	wait

unit_tests:
	@echo " *** Running Category DNA unit tests..."
	@spark-submit ${SPARK_UNITTEST_OPTIONS} tests/unit/category_DNA_unit.py & \
	wait

tests: unit_tests