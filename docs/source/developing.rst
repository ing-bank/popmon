===========================
Developing and Contributing
===========================

Working on the package
----------------------
You have some cool feature and/or algorithm you want to add to the package. How do you go about it?

First clone the package.

.. code-block:: bash

  git clone https://github.com/popmon/popmon.git

then

.. code-block:: bash

  pip install -e popmon/

this will install ``popmon`` in editable mode, which will allow you to edit the code and run it as
you would with a normal installation of the ``popmon`` package.

To make sure that everything works first execute the unit tests.
For this you'll need to install our test requirements:

.. code-block:: bash

  cd popmon/
  pip install -r requirements-test.txt
  python setup.py test

That's it!


Contributing
------------

When contributing to this repository, please first discuss the change you wish to make via issue, email, or any
other method with the owners of this repository before making a change. You can find the contact information on the
`index <index.html>`_ page.

When contributing all unit tests have to run successfully.
