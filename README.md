# QDB

Python is at the heart of most data analysis but to share analysis's most users have been uploading data to SQL databases, until now.
QDB turns your python instance into an SQL database. Existing tools can query data directly at faster than database speeds.

- QDB provides a MySQL interface that allows every BI tool to just work.
- With DuckDB and Polars-SQL you can directly query in-memory dataframes, S3 or on-disk parquet files using SQL.
- Via the simple SQL editor web interface, you can share links to results with colleagues.

Modern tools and libraries are arriving and providing capabilities and performance only previously available accessible by paying for 
huge enterprise licenses $$$. Given these changes, We believe data analysts will increasingly be performed using these tools in future.

### Facts

- **Python** is the language for data analysis.
- **Polars** / **DuckDB** provides extremely fast DataFrame operations previously only accessible at great cost.
- Many libraries and databases are converging on common data formats
    - Apache **Arrow** in memory
    - Apache **Parquet** on disk


# Commands
```
poetry install --sync
poetry lock
poetry run qdb
poetry run pytest
poetry run pyinstaller --onefile  launcher.py
```

# TODO

1. Pypi upload
2. Web - to_json - tests for every data combo.
3. MySQL - tests for every data combo query.
4. Test windows .exe

## Optional TODO

- Ability to set duckdb conn to one of users choosing.
- Ability to set polars context to one of users choosing.

- QWEB
	- autocomplete for duckdb / polars / python?
	- Sharing link to show correct chart type fix.
	- Server Tree Working
- MySQL
	- Test with tableau / dbeaver / ?
	- Server Tree Working
	- Security - Allow setting authentication function
- Tutorials/Website
	- Using QDB to enable DuckDB IPC
	- Using QDB to query python direct from tableau / every popular combo......
	- Using QDB to enable polars SQL IPC
- Deploy
	- Provide mac built package
	- Provide linux built package
	- Get into linux package managers
	- Get into mac package managers 
- KDB
	- Subscribe to streaming data
	- KDB Server ability to allow users to send data in from a kdb+ process?
	- Q Language?
- Misc
    - K Language?
    - Other languages?