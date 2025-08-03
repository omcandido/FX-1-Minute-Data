.PHONY: download-all-raw dt_download dt_clean dt_run

download-all-raw:
	uv run download_all_fx_data.py

dt_download:
	uv run dt_download.py

dt_clean:
	uv run dt_clean.py

dt_run: dt_download dt_clean