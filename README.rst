********
nrt-data
********

**nrt-data** is a companion package to `nrt <https://github.com/ec-jrc/nrt>`_, designed for near real-time monitoring of satellite image time series. It provides easy access to curated datasets for testing and demonstrating **nrt**'s capabilities.

As of ``nrt==0.2.1``, the ``data`` module has been separated from the core package to simplify maintenance and keep the core **nrt** lightweight. This package is distributed as a namespace package, ensuring backward compatibility with previous versions.

For more details, see the full documentation at `nrt-data.readthedocs.io <https://nrt-data.readthedocs.io/>`_.

Features
========

- Access to small-sized test data in NetCDF format and associated reference data in FlatGeoBuf, managed via `Pooch <https://www.fatiando.org/pooch/latest/>`_.
- Streaming access to larger datasets stored as cloud-optimized Zarr stores.
- Synthetic data simulation functionalities.

Installation
============

To install **nrt-data**, run:

.. code-block:: bash

    pip install nrt-data

Please note that **nrt-data** can be installed independently of **nrt** but is incompatible with ``nrt<=0.2.1``. To check your current **nrt** version, run:

.. code-block:: bash

    pip freeze | grep nrt

If necessary, update **nrt** by running:

.. code-block:: bash

    pip install -U nrt

