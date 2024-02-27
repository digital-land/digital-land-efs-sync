piptool-compile::
	python -m piptools compile --output-file=requirements/requirements.txt requirements/requirements.in
	python -m piptools compile requirements/dev-requirements.in

init::
	pip install -r requirements/dev-requirements.txt


test:: test-integration test-acceptance

test-integration::
	python -m pytest tests/integration

test-acceptance::
	python -m pytest tests/acceptance

dbhash::
	mkdir -p ./bin
	cd ./bin; \
	curl -o sqlite.tar.gz https://www.sqlite.org/src/tarball/sqlite.tar.gz; \
	tar -xzvf sqlite.tar.gz; \
	rm -rf sqlite.tar.gz;\
	cd ./sqlite; \
	./configure; \
	make dbhash 