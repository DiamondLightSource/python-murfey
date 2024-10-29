# Murfey Database

Murfey has been designed to work with PostgreSQL databases to maintain a record of the files it has transferred, which allow it to oversee and manage multi-step data processing workflows. This page will walk readers through the process of setting up a PostgreSQL database for use with Murfey.

## Setting Up

### Installing `postgresql`

To start, we will install the `postgresql` Python package hosted on conda-forge. This can be done using either `conda` or `mamba`. In this example, we will create a new Python 3.9 environment named `murfey-db` which will be used to run `postgresql`.

```text
(base)$ mamba create -n murfey-db python==3.9.*
(base)$ mamba activate murfey-db
(murfey-db)$ mamba install -c conda-forge postgresql
```

### Creating and initialising the database

Next, we need to find a directory for the database to reside in. This can generally be any location in your file system, but for this example, with a Unix OS as reference, we shall create a directory called `murfey` in the default home location of `~`.

```text
(murfey-db)$ mkdir -p ~/murfey
(murfey-db)$ cd ~/murfey
(murfey-db)$ initdb -U murfey -D database -W
```

With the command above, we are creating a new superuser named 'murfey' for this database in the folder 'database', which resides in `~/murfey`. The `-W` flag will prompt for a password to be set for this new superuser.

This database cluster can then be started by running the following command:

```text
(murfey-db)$ pg_ctl start -D database -l database.log
```

This will create a background process on your computer that will run this database instance. The `-l` flag in the command specifies the creation of a running log file, which we've named `database.log`. If you wish to stop the database process, you can simply run the following:

```text
(murfey-db)$ pg_ctl stop -D database
```

After that, we can proceed with creating the actual database for Murfey within this cluster using the following command:

```text
createdb murfey --owner=murfey -U murfey -W
```

This wil create a database named 'murfey' owned by the user 'murfey'. This database can then be accessed and inspected by running the following command:

```text
(murfey-db)$ psql murfey -U murfey -W
```

To strengthen the security of your database, you can make password requests mandatory by editing the file `database/pg_hba.conf`. Change all instances of `trust` to `md5` or another one of the listed valid encryption methods.

### Configuring Murfey

At this point, we will need to configure Murfey to be able to connect to the database. To do so, we will need to create a configuration file for the instrument Murfey is supporting, a security configuration file, and a credentials file for the database. Instructions on how to do so can be found here (COMING SOON).

### Configuring the database

Next, we will need to create a role in the Murfey database under whose name the data will be registered. To do so, we start up `postgresql` and run the following commands:

```text
(murfey-db)$ psql murfey -U murfey -W
murfey=# create user murfey_server with password '[password here]' createdb;
murfey=# grant all privileges on database murfey to murfey_server;
murfey=# grant all privileges on schema public to murfey_server;
```

The user 'murfey_server' will be stored under the 'username' key in the database credentials file used for this microscope, and the database name 'murfey' is stored under the 'database' key in that same file. The password you use when creating this role should be the encrypted one set up when creating the credentials file.

### Configuring Murfey again

Having created the the database user, we now need to go back to the Python environment we installed Murfey in in order to perform some final configurations.

```text
(base)$ mamba activate murfey
(murfey)$ murfey.create_db
(murfey)$ murfey.add_user -u '[your choice of username]' -p '[your choice of password]'
```

`murfey.create_db` will populate the 'murfey' PostgreSQL database with the tables needed to record your experimental data, and `murfey.add_user` creates a user in the 'murfeyuser' table that is authorised to connect to this Murfey server.

With this final step, the PostgreSQL database is ready to be used. Once you have installed Murfey on the client computer (COMING SOON), and have setup the relevant configuration files (COMING SOON), Murfey will be ready to support your data transfer and processing needs.
