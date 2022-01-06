# go-pico-sql
A lightweight SQL database written in Go for prototyping and fast development. Although at this moment this database is not indended for the production usage, it should be easy to mitgrate to something more mature.
The database can be built as a docker image.

## Features
- In-memory database with persistence, it's fast but all data have to fit in memory.
- Log based database, compacting done automatically in the background
- REST API. You can talk to the db using `curl`
- Limited SQL support 
  - Supported (in basic forms): SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, CREATE INDEX
  - Not supported: JOIN, GROUP, ORDER, UNION, VIEW, etc
- Persistence based on text files (JSON and CSV) which means easy management, monitoring and troubleshooting
- Go driver (in progress)
- Docker ready

## Building
A makefile script is used for building the database and all dependencies.
```bash
# test & build all
$ make
# test & build all #2
$ make all
# build all
$ make build
# test all
$ make test
# open test coverage report in your default browser
$ make test-cov-html
# clean all
$ make clean
# run database engine (only for testing purposes)
$ make run
# build docker image
$ make build-docker
# run integration tests (more like smoke tests now) in docker (docker-compose required)
$ integration-tests-docker-compose
```
In addition to the above, you may want to rebuild all dependencies (which will take more time):
```bash
$ REBUILD=1 make build
```