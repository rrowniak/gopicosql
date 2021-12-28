# go-pico-sql
A lightweight SQL database written in Go for prototyping and fast development. Although at this moment this database is not indended for the production usage, it should be easy to mitgrate to something more mature.

## Features
- In-memory database with persistence, it's fast but all data have to fit in memory.
- Log based database, compacting done automatically in the background
- REST API. You can talk to the db using `curl`
- Limited SQL support 
  - Supported (in basic forms): SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, CREATE INDEX
  - Not supported: JOIN, GROUP, ORDER, UNION, VIEW, etc
- Persistence based on text files (JSON and CSV) which means easy management, monitoring and troubleshooting
- Go driver (in progress)