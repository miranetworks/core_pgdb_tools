debian/debian.mk: ;

.PHONY: deb_prep deb

APP_VERSION = $(shell sed -n '/vsn/{s/^.*"\(.\+\)".*/\1/;p}' src/pgdb_tools.app.src)
ERL_VERSION = $(shell sed -n '/ERL_VERSION=/{s/^.*=//;p}' debian/erlang_env.sh)

deb_prep:
	for f in debian/*.$(DEB_RELEASE); do cp -v $$f $${f%.*}; done
	for f in debian/*.template; do \
	    echo "Processing $$f"; \
	    sed -e 's/#DEB_VERSION#/$(APP_VERSION)/g' \
	        -e 's/#ERL_VERSION#/$(ERL_VERSION)/g' \
	        -e 's/#DEB_DATETIME#/$(shell date -u --rfc-2822)/g' $$f >$${f%.*}; \
	done
	ls debian/control  # do not remove

deb:
	. debian/erlang_env.sh; dpkg-buildpackage -b -us -uc
	mv ../pgdb-tools_$(APP_VERSION)_*.deb .
