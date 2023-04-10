build:
	docker-compose build --no-rm --parallel

db:
	docker-compose up -d --remove-orphans postgres

test: db
	docker-compose run --rm ftmstore pytest -s tests

stop:
	docker-compose down --remove-orphans