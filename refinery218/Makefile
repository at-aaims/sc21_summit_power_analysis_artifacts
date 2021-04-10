# Globals

# Input from others to minimize change to the Makefile
REPO_NAME ?= $(shell . ./.config.env && echo $${REPO_NAME})
ACCOUNT ?= $(shell . ./.config.env && echo $${ACCOUNT})
GROUP ?= $(shell . ./.config.env && echo $${GROUP})
USER ?= $(shell whoami)

# Fixed locations based on a common convention
REPO_SCRATCH=/gpfs/alpine/$(ACCOUNT)/scratch/$(USER)/.tmp/$(REPO_NAME)
DVC_CACHE=/gpfs/alpine/$(ACCOUNT)/proj-shared/data/dvc.cache/$(REPO_NAME)
DVC_REPO=/gpfs/alpine/$(ACCOUNT)/proj-shared/data/dvc.repo/$(REPO_NAME)
DVC_REPO_ARCHIVE=/hpss/prod/$(ACCOUNT)/proj-shared/data/dvc.repo/$(REPO_NAME)
DATALAKE=/gpfs/alpine/$(ACCOUNT)/proj-shared/data/lake

# OLCF specific cluster name identifier
define SYSID
case `hostname -f` in \
login*.summit.*) echo "summit";; \
andes*) echo "andes";; \
dtn*) echo "dtn";; \
jupyter*) echo "jupyter";; \
*) echo `uname -m`;; \
esac
endef

# Hostname
SYSNAME=$(shell $(SYSID))
VENV=./.venv.$(SYSNAME)

.PHONY: all help
help:
	-@echo ""
	-@echo "[$(REPO_NAME)]"
	-@echo ""
	-@echo "Setup"
	-@echo "  init|fini: init / fini python virtual environment"
	-@echo "    (this is only necessary only if you're working in a standalone"
	-@echo "     scenario working on the registry itself)"
	-@echo "  dvc-config: Configure the dvc repository"
	-@echo "  env: utility activating the environment"
	-@echo ""
	-@echo "Python library development"
	-@echo "  test-(verbose): run unit tests"
	-@echo "  integrate: run integration tests"
	-@echo ""
	-@echo "Archive:"
	-@echo "  archive: push repository to archive (sync)"
	-@echo "  restore: restore repository from archive (sync)"
	-@echo ""
	-@echo "Environment"
	-@echo "  user: $(USER)"
	-@echo "  account: $(ACCOUNT)"
	-@echo "  cache: $(DVC_CACHE)"
	-@echo "  repository: $(DVC_REPO)"
	-@echo "  archive: $(DVC_REPO_ARCHIVE)"
	-@echo "  datalake: $(DATALAKE)"
	-@echo ""

all: help

.PHONY: init pip fini env dvc-config
init:
	@./clusters/$(SYSNAME)/init.sh
	@echo "@ initialized virtual environment at $(VENV)"

fini:
	@echo "@ removing python virtual environment at $(VENV)"
	-@rm -rf $(VENV)

env:
	-@echo ". ./clusters/$(SYSNAME)/env.sh"

dvc-config:
	-@echo "@ Ensuring DVC configuration for the registry"
	dvc config cache.type hardlink,symlink
	dvc config cache.shared group
	dvc cache dir $(DVC_CACHE)
	dvc remote add -f -d master $(DVC_REPO)
	dvc remote add -f datalake $(DATALAKE)
	dvc remote list
	@echo "@ Commit changes if necessary"

.PHONY: test test-verbose
test:
	$(VENV)/bin/pytest

test-verbose:
	$(VENV)/bin/pytest -sv

.PHONY: archive restore
archive:
	-@echo "@ archive the repository to HPSS"

restore:
	-@echo "@ restore data from HPSS"

