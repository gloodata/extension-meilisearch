ms_master_key := env('MS_MASTER_KEY')

start-meilisearch:
    ./meilisearch --master-key {{ms_master_key}} --config-file-path ./ms-config.toml
