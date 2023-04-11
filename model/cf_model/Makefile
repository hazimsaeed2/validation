# Make recipes for CF pipeline


# 1. Setup
.PHONY: list etl etl_future train tune predict combine predict_combined run full_run test

list:
	@grep '^[^#[:space:]].*:' Makefile


# 2. Define recipe routines

define do_etl
	spark-submit --num-executors 59 --executor-cores 15 --executor-memory 32G --py-files ./lib/cf_utils.py,./lib/cf_io.py ./scripts/cf_etl.py --conf ./conf/$(1)
endef

define do_etl_future
	spark-submit --num-executors 59 --executor-cores 15 --executor-memory 32G --py-files ./lib/cf_utils.py,./lib/cf_io.py ./scripts/cf_etl.py --conf ./conf/$(1) --future # Future is called in cf_io.py
endef

define do_train
	spark-submit --num-executors 59 --executor-cores 15 --executor-memory 32G --py-files ./lib/cf_utils.py,./lib/cf_io.py,./lib/models.py,./lib/cf_metrics.py,./lib/backtest.py ./scripts/als_train.py --conf ./conf/$(1)
endef

define do_train_rmse
	spark-submit --num-executors 59 --executor-cores 15 --executor-memory 32G --py-files ./lib/cf_utils.py,./lib/cf_io.py,./lib/models.py,./lib/cf_metrics.py,./lib/backtest.py ./scripts/als_train.py --conf ./conf/$(1) --rmse
endef

define do_tune
	spark-submit --num-executors 59 --executor-cores 15 --executor-memory 32G --py-files ./lib/cf_utils.py,./lib/cf_io.py,./lib/models.py,./lib/cf_metrics.py,./lib/backtest.py ./scripts/als_tune.py --conf ./conf/$(1)
endef

define do_predict
	spark-submit --num-executors 59 --executor-cores 15 --executor-memory 32G --py-files ./lib/cf_utils.py,./lib/cf_io.py,./lib/models.py,./lib/cf_metrics.py,./lib/backtest.py ./scripts/cf_predict.py --conf ./conf/$(1)
endef

define do_predict_future
	spark-submit --num-executors 59 --executor-cores 15 --executor-memory 32G --py-files ./lib/cf_utils.py,./lib/cf_io.py,./lib/models.py,./lib/cf_metrics.py,./lib/backtest.py ./scripts/cf_predict.py --conf ./conf/$(1) --future
endef

define do_combine
	spark-submit --num-executors 59 --executor-cores 15 --executor-memory 32G --py-files ./lib/cf_io.py, ./scripts/cf_combine_preds.py --conf ./conf/$(1)
endef

define do_run
	$(call do_etl,$(config))
	$(call do_etl_future,$(config))
	$(call do_train,$(config))
	$(call do_predict_future,$(config))
endef

define do_prod_run
	$(call do_etl,$(config))
	$(call do_train,$(config))
	$(call do_predict,$(config))
endef

define do_prod_run_rmse
	$(call do_etl,$(config))
	$(call do_etl_future,$(config))
	$(call do_train_rmse,$(config))
	$(call do_train,$(config))
	$(call do_predict,$(config))
endef

# 3. List recipe options

etl:
	$(call do_etl,$(config))

etl_future:
	$(call do_etl_future,$(config))

train:
	$(call do_train,$(config))
train_rmse:
	$(call do_train_rmse,$(config))
tune:
	$(call do_tune,$(config))

predict:
	$(call do_predict,$(config))

predict_future:
	$(call do_predict_future,$(config))

combine:
	$(call do_combine,$(config))

run:
	$(call do_run,$(config))

prod_run:
	$(call do_prod_run,$(config))

prod_run_rmse:
	$(call do_prod_run_rmse,$(config))

full_run:
	for config in $(config_AH4) $(config_AH5); do\
		$(call do_etl,$$config);\
		$(call do_train,$$config);\
		$(call do_predict,$$config);\
	done
	 $(call do_combine,$$config_AH4)

full_prod_run:
	for config in $(config_AH4) $(config_AH5); do\
		$(call do_etl,$$config);\
		$(call do_etl_future,$$config);\
		$(call do_train_rmse,$$config);\
		$(call do_train,$$config);\
		$(call do_predict,$$config);\
	done
	$(call do_combine,$$config_AH4)

test:
	spark-submit --num-executors 59 --executor-cores 15 --executor-memory 32G --py-files ./lib/cf_utils.py,./lib/cf_io.py ./scripts/cf_etl.py --test
	spark-submit --num-executors 59 --executor-cores 15 --executor-memory 32G --py-files ./lib/cf_utils.py,./lib/cf_io.py,./lib/models.py,./lib/cf_metrics.py,./lib/backtest.py ./scripts/als_train.py --test
	spark-submit --num-executors 59 --executor-cores 15 --executor-memory 32G --py-files ./lib/cf_utils.py,./lib/cf_io.py,./lib/models.py,./lib/cf_metrics.py,./lib/backtest.py ./scripts/cf_predict.py --test
