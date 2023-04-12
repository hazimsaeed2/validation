# 1. Setup
.PHONY: list etl create_rf_spend_model create_rf_trip_model grid-search_trip grid-search_spend create_models predict

config = config.yml

list:
	@grep '^[^#[:space:]].*:' Makefile


# 2. define recipes

etl:
	spark-submit ./scripts/ETL.py $(config)

create_rf_spend_model:
	python3 ./scripts/sklearn_spend_model.py $(config)

create_rf_trip_model:
	python3 ./scripts/sklearn_trip_propensity_model.py $(config)

grid-search_trip:
	python3 ./scripts/sklearn_grid_search.py $(config)

grid-search_spend:
	python3 sklearn_grid_search.py $(config)

create_models:
	python3 ./scripts/sklearn_trip_propensity_model.py $(config)
	python3 ./scripts/sklearn_spend_model.py $(config)

predict:
	python3 ./scripts/predict.py $(config)

models_predict:
	python3 ./scripts/sklearn_trip_propensity_model.py $(config)
	python3 ./scripts/sklearn_spend_model.py $(config)
	python3 ./scripts/predict.py $(config)