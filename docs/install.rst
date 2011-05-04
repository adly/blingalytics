Installation
============

Install with pip
----------------

Simply use pip_ to install the blingalytics package::

    pip install blingalytics

Install from source
-------------------

Download or clone the source from Github and run setup.py install::

    git clone git@github.com:adly/blingalytics.git
    cd blingalytics
    python setup.py install

Requirements
------------

For a very basic infrastructure, Python is the only dependency:

* Python_ >= 2.5

Some of the caches and sources have other dependencies, but they are only
required if you're actually using those particular source and cache engines.
The documentation for that module will specify what its dependencies are,
which may include:

* Redis_
* SQLAlchemy_
* Elixir_

.. _pip: http://www.pip-installer.org/
.. _Python: http://www.python.org/
.. _Redis: http://redis.io/
.. _SQLAlchemy: http://www.sqlalchemy.org/
.. _Elixir: http://elixir.ematia.de/trac/wiki
