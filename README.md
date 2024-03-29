# Spark ImageJ ![](https://github.com/kmader/spark-ij/workflows/compile/badge.svg) ![](https://github.com/kmader/spark-ij/workflows/package/badge.svg) ![](https://github.com/kmader/spark-ij/workflows/tests/badge.svg)

A Spark interface for using ImageJ tools including running plugins, loading images (using the
legacy methods) and running basic analyses which produce table outputs and can be collated.

## PortableImagePlus

The flexible format for carrying around ImagePlus objects inside of Spark so there are no issues
with serialization (https://gist.github.com/kmader/1d64e64621e63d566f67)

## ImageLog

A simple class for logging operations on an image to they can be tracked and eventually
reproduced later.
