#!/bin/bash
. ./.config.env
if [ ! -e ./${REPO_NAME} ]; then
	mkdir -p ${REPO_NAME}; touch ${REPO_NAME}/__init__.py
fi
if [ -e ./setup.py -a -e ./${REPO_NAME} ]; then \
	pip install --upgrade pip; \
	pip install -e . ; \
fi
