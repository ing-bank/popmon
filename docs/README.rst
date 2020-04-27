Generating Documentation with Sphinx
====================================

This README is for generating and writing documentation using Sphinx.
On the repository there should already be the auto-generated files
along with the regular documentation.

Installing Sphinx
-----------------

First install Sphinx. Go to http://www.sphinx-doc.org/en/stable/ or run

::

    pip install -U Sphinx
    pip install -U sphinx-rtd-theme
    conda install -c conda-forge nbsphinx

The eskapade/docs folder has the structure of a Sphinx project.
However, if you want to make a new Sphinx project run:

::

    sphinx-quickstart

It quickly generates a conf.py file which contains your configuration
for your sphinx build.

Update the HTML docs
--------------------

Now we want Sphinx to autogenerate from docstrings and other
documentation in the code base. Luckily Sphinx has the apidoc
functionality. This goes through a path, finds all the python files and
depending on your arguments, parses certain parts of the code
(docstring, hidden classes, etc.).

**First make sure your environment it setup properly. Python must be
able to import all modules otherwise it will not work!**

From the the root of the repository:

::

    $ source setup.sh

To run the autogeneration of the documentation type in /docs/:

::

    ./autogenerate.sh

to scan the pyfiles and generate \*.rst files with the documentation.
The script itself contains the usage of apidoc.

Now to make the actual documentation files run:

::

    make clean

to clean up the old make of sphinx and run:

::

    make html

to make the new html build. It will be stored in (your config can adjust
this, but the default is:) docs/build/html/ The index.html is the
starting page. Open this file to see the result.

Mounting a different repository to vagrant
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you want to develop code that is not part of the repository that 
your vagrant is in, you can mount it seperately. This is done by changing
the Vagrantfile, by changing the ``#mount`` line to the path of the repository
that you want to mount:

::

  config.vm.synced_folder "<PATH_TO_REPOSITORY>", "<LOCATION_TO_MOUNT>", id: "esrepo"
    
where the location to mount is e.g. /opt/eskapade.

What is an .rst file?
~~~~~~~~~~~~~~~~~~~~~

R(e)ST is the format that Sphinx uses it stands for ReSTructured
(http://docutils.sourceforge.net/docs/user/rst/quickref.html). It looks
for other RST files to import, see index.rst to see how the **toctree**
refers to other files.
