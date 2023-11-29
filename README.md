Surello: SurrealDB loader
=========================

    Status: script kiddy hackery

Commandline tool to get started with data in SurrealDB by allowing you to simply execute surql and inject data into SurrealDB.

Surello can:
- Execute scripts
- Load CSV files
- Load Parquet files

Surello will:
- Keep a list of executed and loaded files
- Hash input so you can verify changes
- Drop changes from the change history

Surello will not drop changes.
