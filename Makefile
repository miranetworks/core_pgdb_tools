
.PHONY: all ctags app clean test deps analyze

# Default build target
all: clean deps ctags
	./rebar compile

ctags:
	- ctags -R src/ deps/ test/

deps:
	./rebar get-deps

clean:
	rm -fr .eunit
	rm -fr erl_crash.dump
	./rebar clean

test: all
	sudo /etc/init.d/postgresql start
	sudo -u postgres psql -f test/bootstrap_database.sql
	mkdir -p .eunit
	./rebar skip_deps=true eunit

analyze: all
	dialyzer ebin/ --fullpath
include docker/docker.mk
