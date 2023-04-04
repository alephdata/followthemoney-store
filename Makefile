build:
	docker build -t followthemoney-store .

test:
	docker run --rm followthemoney-store pytest