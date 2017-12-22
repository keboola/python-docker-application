Python Docker application
-----------------

[![Build Status](https://travis-ci.org/keboola/python-docker-application.svg?branch=master)](https://travis-ci.org/keboola/python-docker-application)
[![Code Climate](https://codeclimate.com/github/keboola/python-docker-application/badges/gpa.svg)](https://codeclimate.com/github/keboola/python-docker-application)

General library for python application running in KBC. The library provides function related to [Docke Runner](https://github.com/keboola/docker-bundle).

Installation
===============

```
pip3 install git+git://github.com/keboola/python-docker-application.git
```

To upgrade existing installation use:

```
pip3 install --upgrade --force-reinstall git+git://github.com/keboola/python-docker-application.git@2.0.0
```

Note that the library is already installed in production containers in KBC.


Usage
============
Basic usage:
```
from keboola import docker
cfg = docker.Config('/data/')
params = cfg.get_parameters()
```

See documentation [in doc directory](https://github.com/keboola/python-docker-application/tree/master/doc) for full list of available functions. See [development guide](http://developers.keboola.com/extend/custom-science/python/) for help with KBC integration.
