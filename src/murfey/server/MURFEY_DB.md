# Murfey Database

Murfey has been designed to work with PostgreSQL databases to maintain a record of the files it has transferred in order to allow for multi-step data processing workflows. This page will walk readers through the process of setting up a PostgreSQL database for use with Murfey.

## Setting Up the Database

### Install `postgresql`

To start, we will install the `postgresql` Python package hosted on conda-forge. This can be done using either `conda` or `mamba`. In this example, we will create a new Python 3.9 environment named `murfey-db` which will be used to run `postgresql`.

```bash
$ mamba create -n murfey-db python==3.9.*
$ mamba activate murfey-db
(murfey-db)$ mamba install -c conda-forge postgresql
```

### Creating the database directory

Next, we need to find a directory for the database to reside in. This can generally be any location in your file system, but for this example, with a Unix OS, we shall create a directory called `murfey` in the default home location of `~`.

```bash
(murfey-db)$ mkdir -p ~/murfey
(murfey-db)$ cd ~/murfey
```
