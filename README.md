# Murfey

A transporter for data from Diamond eBIC microscope and detector machines onto the Diamond network.

### Who is Murfey?

Murfey, the package, is named after [Eliza Murfey, the inventor](https://nationalrrmuseum.org/blog/mother-of-invention-women-railroad-innovators/):

> Focusing on the mechanics of the railroad, inventor Eliza Murfey created devices designed
> to improve the bearings on rail wheels. With sixteen patents for her lubrication system,
> it was Murfey who designed the packings that would lubricate the axles with oil, aiding
> in the reduction of derailments caused by seized axles and bearings.

### Installation

Murfey itself is installable via `pip`. There are separate dependencies for the server and client components:

```bash
$ pip install murfey[client]
$ pip install murfey[server]
```

Murfey uses a database to store transfer information. A [PostgreSQL](https://www.postgresql.org/) server is
therefore required. Once the server is running and a database has been created the credentials should be stored
in a YAML file with the following format (items in curly braces should be replaced)

```bash
username: {username}
password: {encrypted password}
host: {database server IP}
port: {database server port, default 5432 for PostgreSQL}
database: {name of the database you made}
```

This credentials file shoud be pointed to from the Murfey machine configuration file under `murfey_db_credentials`.

### How do I set up a development environment?

We suggest you start with your favourite virtual environment (mamba/conda/python virtualenv/...),
then install using the following command.

#### From Git

```bash
$ git clone git@github.com:DiamondLightSource/python-murfey.git
$ cd python-murfey
$ pip install -e .[client,server,developer]
```

The packages included under the `[developer]` installation key contain some helpful tools to aid you with developing Murfey further:

- `ipykernel` - Enables interactive code development via Jupyter Notebooks.
- `pre-commit` - Allows for the installation and running of hooks to help with linting, formatting, and type checking your code.
- `pytest` - Used in conjunction with test functions to evaluate the reliability of your code.
- `bump2version` - A nice little script to simplify version control.

Finally, you may want to set up an ISPyB mock database server and a Zocalo
development environment. The instructions for this are out of scope here.

You can then start the Murfey server with

```bash
$ murfey.server
```

and connect the client with

```bash
$ murfey --server http://127.0.0.1:8000
```

You can also install a client on a remote machine. This machine only needs to have
a minimum Python installation and curl. Open the murfey server website in a browser
on the client, and navigate to the bootstrap page. Then copy the displayed commands
into a command line terminal.
