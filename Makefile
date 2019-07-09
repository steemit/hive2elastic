REPO_SLUG=steemit/hive2elastic #TODO: Make this shell out to git remove -v | grep blah
VERSION=$(shell cat VERSION)

build:
	docker build -t ${REPO_SLUG}:${VERSION} .
