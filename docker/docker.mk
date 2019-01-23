docker/docker.mk: ;

.PHONY: ddockerclean dbash dmake drun ddeb

DOCKER_IMAGE = pgdb_tools
DOCKER_CONTAINER_NAME = pgdb_tools
DOCKER_BUILT = docker/.built

$(DOCKER_BUILT): docker/Dockerfile
	docker build -t $(DOCKER_IMAGE) docker/ && touch $(DOCKER_BUILT)

ddockerclean:
	rm -f $(DOCKER_BUILT)

dbash: $(DOCKER_BUILT)
	docker/run $(DOCKER_IMAGE) $(DOCKER_CONTAINER_NAME) "/bin/bash" -t

dmake: $(DOCKER_BUILT)
	docker/run $(DOCKER_IMAGE) $(DOCKER_CONTAINER_NAME) "make"

drun: $(DOCKER_BUILT)
	docker/run $(DOCKER_IMAGE) $(DOCKER_CONTAINER_NAME) "make run" -t -p 2666:2666 -p 2667:2667

dtest: $(DOCKER_BUILT)
	docker/run $(DOCKER_IMAGE) $(DOCKER_CONTAINER_NAME) "make test suite=${suite}" -p 2666:2666 -p 2667:2667

d%: $(DOCKER_BUILT)
	docker/run $(DOCKER_IMAGE) $(DOCKER_CONTAINER_NAME) "make $(@:d%=%)"

ddeb:
	docker/run registry.miranetworks.net/miranetworks/ppa_$(PPA_DISTRO)_$(PPA_RELEASE) deb_$(PPA_DISTRO)_$(PPA_RELEASE)_$(DOCKER_CONTAINER_NAME) "build_deb" -e PPA_RELEASE=$(PPA_RELEASE)
