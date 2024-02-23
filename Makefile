piptool-compile::
	python -m piptools compile --output-file=requirements/requirements.txt requirements/requirements.in
	python -m piptools compile requirements/dev-requirements.in

init::
	pip install -r requirements/dev-requirements.txt

test::
	python -m pytest tests/acceptance