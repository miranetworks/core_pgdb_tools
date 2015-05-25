###Docker targets
dbuild: docker/Dockerfile.template
	cd docker; ./build $(if $(nocache),"--no-cache")

dcibuild: dbuild
	rm -fr deps
	cd docker; ./run_for_ci "make test"

dtest: dbuild
	cd docker; ./run_test "make test"

dclean: dbuild
	cd docker; ./run "make clean"

drun: dbuild
	cd docker; ./run "make run"

dbash: dbuild
	cd docker; ./run "/bin/bash"

