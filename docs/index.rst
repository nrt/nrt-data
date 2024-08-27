.. nrt-data documentation master file, created by
   sphinx-quickstart on Tue Aug 27 12:28:54 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to nrt-data's documentation!
====================================

**nrt-data** is a companion package to `nrt <https://github.com/ec-jrc/nrt>`_, designed for near real-time monitoring of satellite image time series. It provides easy access to curated datasets for testing and demonstrating **nrt**'s capabilities.

As of ``nrt==0.2.1``, the ``data`` module has been separated from the core package to simplify maintenance and keep the core **nrt** lightweight. This package is distributed as a namespace package, ensuring backward compatibility with previous versions.


Features
--------

- Access to small-sized test data in NetCDF format and associated reference data in FlatGeoBuf, managed via `Pooch <https://www.fatiando.org/pooch/latest/>`_.
- Streaming access to larger datasets stored as cloud-optimized Zarr stores.
- Synthetic data simulation functionalities.



.. automodule:: nrt

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   quickstart
   api_reference


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
