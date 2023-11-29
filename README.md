Surello: SurrealDB loader
=========================

    Status: script kiddy hackery

Commandline tool to get started with data in SurrealDB by allowing you to simply execute surql and inject data into SurrealDB.

Surello goals:
- [x] Execute scripts
- [ ] Load CSV files
- [ ] Load Parquet files
- [x] Keep a list of executed and loaded files
- [ ] Hash input so you can verify changes
- [ ] Drop changes from the change history to force reload

Surello will not drop changes.
