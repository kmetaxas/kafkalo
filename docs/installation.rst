.. highlight:: shell

============
Installation
============


Stable release
--------------

To install Kafkalo, run this command in your terminal:

.. code-block:: console

    $ pip install kafkalo

This is the preferred method to install Kafkalo, as it will always install the most recent stable release.

If you don't have `pip`_ installed, this `Python installation guide`_ can guide
you through the process.

.. _pip: https://pip.pypa.io
.. _Python installation guide: http://docs.python-guide.org/en/latest/starting/installation/


The default `confluent python`_ package bundles librdkafka without GSSAPI support. If you need GSSAPI support follow the documentation in the confluent python page. 
This typically requires you to install gcc+dev tools, librdkafka headers and python headers and run: `pip install --no-binary :all: confluent-kafka`


From sources
------------

The sources for Kafkalo can be downloaded from the `Github repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone git://github.com/kmetaxas/kafkalo

Or download the `tarball`_:

.. code-block:: console

    $ curl -OJL https://github.com/kmetaxas/kafkalo/tarball/master

Once you have a copy of the source, you can install it with:

.. code-block:: console

    $ python setup.py install


.. _Github repo: https://github.com/kmetaxas/kafkalo
.. _tarball: https://github.com/kmetaxas/kafkalo/tarball/master
.. _confluent python: https://github.com/confluentinc/confluent-kafka-python#install
