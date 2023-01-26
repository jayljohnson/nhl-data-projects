all:

load_db:
	mkdir -p data/outputs/sqlite
	rm data/outputs/sqlite/nhl.db
	time csvs-to-sqlite \
		data/outputs/feed-live/feed-live.csv \
		data/outputs/feed-live/game-roster-feed-live.csv \
		data/outputs/feed-live/game-summary-feed-live.csv \
		data/outputs/shift-charts/shift-charts.csv \
		data/outputs/analysis/game_results.csv \
		data/outputs/analysis/season_results.csv \
		data/outputs/sqlite/nhl.db


transform_feed_live:
	time python -m src.feed_live.transform_feed_live_to_csv

transform_feed_live_rosters:
	time python -m src.feed_live.transform_feed_live_rosters_to_csv

transform_shift_charts:
	time python -m src.shift_charts.transform_shift_charts_to_csv

datasette:
	datasette data/outputs/sqlite/nhl.db --setting sql_time_limit_ms 20000

CURRENT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
CURRENT_USER := ${USER}
DOCKER_IMAGE_PREFIX := ${shell pwd | awk -F/ '{print $$NF}'}

all: version

version: up
	@echo 'Base directory: ${DOCKER_IMAGE_PREFIX}'
	docker exec -ti ${DOCKER_IMAGE_PREFIX}_application_1 sh -c "echo Python Version: && python3 --version"

up:
	docker-compose up -d
	docker container ps

down:
	docker-compose down

restart:
	docker-compose restart
	docker container ps

build: down
	docker-compose build
	docker-compose up -d

test: up
	docker exec -ti  ${DOCKER_IMAGE_PREFIX}_application_1 coverage run --branch -m pytest
	docker exec -ti  ${DOCKER_IMAGE_PREFIX}_application_1 coverage report

psql: up
	docker exec -ti  ${DOCKER_IMAGE_PREFIX}_db_1 sh -c "psql -h localhost -p 5432 -U postgres -w"

cli: up
	docker exec -ti  ${DOCKER_IMAGE_PREFIX}_application_1 /bin/bash

python: up
	docker exec -ti  ${DOCKER_IMAGE_PREFIX}_application_1 sh -c "python"

permissions:
	sudo chown -R ${USER}:${USER} .git
	sudo chown -R ${USER}:${USER} data
	# Run with -i flag to ignore errors, `make permissions -i`
	sudo groupadd docker
	sudo usermod -aG docker ${USER}
	sudo newgrp docker

