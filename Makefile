run_pipeline:_run_pipeline

_run_pipeline:
	( \
    docker-compose up -d && \
    sleep 10 && \
    pip3 install -r requirements.txt && \
    python3 main.py \
    )