.PHONY: up down logs ps local demo test test-takeover clean chaos-reset

up:
	docker compose up --build

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=50

ps:
	docker compose ps

local:
	.venv/bin/python run_local.py

demo:
	python3 demo.py --twice

test:
	.venv/bin/pytest -v tests

test-takeover:
	RUN_TAKEOVER=1 .venv/bin/pytest -v tests/test_takeover.py

chaos-reset:
	curl -s -X POST localhost:9000/chaos/reset
	@echo
	curl -s -X POST localhost:8000/v1/admin/providers/reset
	@echo

clean:
	rm -rf .data
