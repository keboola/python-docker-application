Python Docker application
-----------------

[![Build Status](https://travis-ci.org/keboola/python-docker-application.svg?branch=master)](https://travis-ci.org/keboola/python-docker-application)
[![Code Climate](https://codeclimate.com/github/keboola/python-docker-application/badges/gpa.svg)](https://codeclimate.com/github/keboola/python-docker-application)

General library for python application running in KBC. The library provides function related to [docker-bundle](https://github.com/keboola/docker-bundle).

Installation
===============

```
pip install git+git://github.com/keboola/python-docker-application.git
```

Note that in production containers in KBC, the library is already installed.

Usage
============
Basic usage:
```
from keboola import docker
cfg = docker.Config('/data/')
params = cfg.getParameters()
```

See [documentation]() for list of available functions.
