.PHONY: download-all-raw download-all-delta update-delta

download-all-raw:
	uv run download_all_fx_data.py

dt_download:
	uv run dt_download.py

dt_update:
	uv run dt_update.py

dt_clean:
	uv run dt_clean.py