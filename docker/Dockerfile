FROM ruanjonker/erlang:ubuntu1404_17.4

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update

RUN apt-get install -y dput
RUN apt-get install -y ctags

RUN mkdir /workspace
RUN useradd -m -d /workspace --uid ${DOCKER_DEV_UID} -s /bin/bash developer
RUN chown -R developer:developer /workspace

RUN adduser developer sudo
RUN echo "%sudo ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

RUN echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /workspace/base

CMD /bin/bash

