ms_master_key := env('MS_MASTER_KEY')

run:
    uv run src/main.py

start-meilisearch:
    ./meilisearch --master-key {{ms_master_key}} --config-file-path ./ms-config.toml
