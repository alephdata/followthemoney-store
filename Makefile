build:
	docker-compose build --no-rm --parallel

db:
	docker-compose up -d --remove-orphans postgres

test:
	docker-compose run --rm ftmstore pytest -s tests

stop:
	docker-compose down --remove-orphans

dist:
	docker-compose run --rm ftmstore python3 setup.py sdist bdist_wheel
