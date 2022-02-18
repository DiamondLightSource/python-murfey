# Murfey

A transporter for data from Diamond eBIC microscope and detector machines onto the Diamond network.


### Who is Murfey?

Murfey, the package, is named after [Eliza Murfey, the inventor](https://nationalrrmuseum.org/blog/mother-of-invention-women-railroad-innovators/):

> Focusing on the mechanics of the railroad, inventor Eliza Murfey created devices designed
> to improve the bearings on rail wheels. With sixteen patents for her lubrication system,
> it was Murfey who designed the packings that would lubricate the axles with oil, aiding
> in the reduction of derailments caused by seized axles and bearings.


### How do I set up a development environment?

We suggest you start with your favourite virtual environment (mamba/conda/python virtualenv/...),
then install the dependencies listed in `requirements_dev.txt` with eg.

```bash
$ git clone git@github.com:DiamondLightSource/python-murfey.git
$ cd python-murfey
$ pip install -r requirements_dev.txt
$ pip install -e .[client,server]
```

You will also want to set up pre-commits:
```bash
$ pip install pre-commit
$ pre-commit install
```

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
