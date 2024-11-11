# Murfey

A transporter for data from Diamond eBIC microscope and detector machines onto the Diamond network.

## Who is Murfey?

Murfey, the package, is named after [Eliza Murfey, the inventor](https://nationalrrmuseum.org/blog/mother-of-invention-women-railroad-innovators/):

> Focusing on the mechanics of the railroad, inventor Eliza Murfey created devices designed
> to improve the bearings on rail wheels. With sixteen patents for her lubrication system,
> it was Murfey who designed the packings that would lubricate the axles with oil, aiding
> in the reduction of derailments caused by seized axles and bearings.

## How do I set up a development environment?

We suggest you start with your favourite virtual environment (mamba/conda/python virtualenv/...),
then install using the following command.

```text
$ git clone git@github.com:DiamondLightSource/python-murfey.git
$ cd python-murfey
$ pip install -e .[client,server,developer]
```

The packages included under the `[developer]` installation key contain some helpful tools to aid you with developing Murfey further:

- `bump-my-version` - Simplifies version control.
- `ipykernel` - Enables interactive code development via Jupyter Notebooks.
- `pre-commit` - Allows for the installation and running of hooks to help with linting, formatting, and type checking your code.
- `pytest` - Used in conjunction with test functions to evaluate the reliability of your code.

Instructions for setting up the database for Murfey to register files to can be found [here](src/murfey/server/README.md).

Finally, you may want to set up an ISPyB mock database server and a Zocalo
development environment. The instructions for this are out of scope here.

You can then start the Murfey server with

```text
$ murfey.server
```

and connect the client with

```text
$ murfey --server http://127.0.0.1:8000
```

You can also install a client on a remote machine. This machine only needs to have
a minimum Python installation and curl. Open the murfey server website in a browser
on the client, and navigate to the bootstrap page. Then copy the displayed commands
into a command line terminal.
