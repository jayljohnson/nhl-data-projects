all:



### GAMES ###
get_games: up
	time docker exec -ti  ${DOCKER_IMAGE_PREFIX}_application_1 sh -c "python -m src.games.extract.season_games"

### FEED LIVE ###
get_feed_live: up
	time docker exec -ti  ${DOCKER_IMAGE_PREFIX}_application_1 sh -c "python -m src.games.extract.feed_live_games"

transform_feed_live_rosters:
	time docker exec -ti  ${DOCKER_IMAGE_PREFIX}_application_1 sh -c "python -m src.feed_live.transform_feed_live_rosters_to_csv"

transform_shift_charts:
	time docker exec -ti  ${DOCKER_IMAGE_PREFIX}_application_1 sh -c "python -m src.shift_charts.transform_shift_charts_to_csv"

datasette:
	datasette data/outputs/sqlite/nhl-feed-live.db --setting sql_time_limit_ms 20000

fly_deploy: get_feed_live
	time datasette publish fly data/outputs/nhl-data.db \
		--app="biscuitbarn" \
		--install datasette-saved-queries \
		--setting sql_time_limit_ms 25000
		# --setting allow_download off  # TODO: Bugfix needed for bools

fly_deploy_incremental:
	# Placeholder -

fly_cli:
	 	nhl-data-projects
	# "pip install csvs-to-sqlite"

fly_restart:
	fly apps restart nhl-data-projects

CURRENT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
CURRENT_USER := ${USER}
DOCKER_IMAGE_PREFIX := ${shell pwd | awk -F/ '{print $$NF}'}

# all: version

version: up
	@echo 'Base directory: ${DOCKER_IMAGE_PREFIX}'
	time docker exec -ti ${DOCKER_IMAGE_PREFIX}_application_1 sh -c "echo Python Version: && python3 --version"

up:
	time docker-compose up -d
	docker container ps

down:
	docker-compose down

restart:
	time docker-compose restart
	docker container ps

build: down
	time docker-compose build
	docker-compose up -d

test: up
	time docker exec -ti  ${DOCKER_IMAGE_PREFIX}_application_1 coverage run --branch -m pytest && coverage report
	# docker exec -ti  ${DOCKER_IMAGE_PREFIX}_application_1 coverage report

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

benchmark:
	time head -c 10G < /dev/urandom > load_test_random.data
	rm load_test_random.data
	time head -c 10G < /dev/urandom > /var/local/load_test_random.data
	rm load_test_random.data > /var/local/load_test_random.data


# DEPRECATED - for reference but not used anymore because direct writes to db instead of csv
load_db: up
	time docker exec -ti  ${DOCKER_IMAGE_PREFIX}_application_1 sh -c "mkdir -p data/outputs/sqlite && \
	rm -f data/outputs/sqlite/nhl.db || true && \
	csvs-to-sqlite \
		data/outputs/feed-live/feed-live.csv \
		data/outputs/analysis/game_results.csv \
		data/outputs/analysis/season_results.csv \
		data/outputs/sqlite/nhl.db"

load_db_old: up
	time docker exec -ti  ${DOCKER_IMAGE_PREFIX}_application_1 sh -c "mkdir -p data/outputs/sqlite && \
	rm -f data/outputs/sqlite/nhl.db || true && \
	csvs-to-sqlite \
		data/outputs/feed-live/feed-live.csv \
		# data/outputs/feed-live/game-roster-feed-live.csv \
		# data/outputs/feed-live/game-summary-feed-live.csv \
		# data/outputs/shift-charts/shift-charts.csv \
		data/outputs/analysis/game_results.csv \
		data/outputs/analysis/season_results.csv \
		data/outputs/sqlite/nhl.db"