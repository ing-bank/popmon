import os
import unittest

import nbformat
import pytest
from jupyter_client.kernelspec import KernelSpecManager, NoSuchKernel
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert.preprocessors.execute import CellExecutionError

from popmon import resources

kernel_name = "python3"

# check if jupyter python3 kernel can be opened. if kernel not found, skip unit tests below.
try:
    km = KernelSpecManager()
    km.get_kernel_spec(kernel_name)
    kernel_found = True
except NoSuchKernel:
    kernel_found = False


class NotebookTest(unittest.TestCase):
    """Unit test notebook"""

    def run_notebook(self, notebook):
        """ Test notebook """

        # load notebook
        with open(notebook) as f:
            nb = nbformat.read(f, as_version=4)

        # execute notebook
        ep = ExecutePreprocessor(timeout=600, kernel_name=kernel_name)
        try:
            ep.preprocess(nb, {})
            status = True
        except CellExecutionError:
            # store if failed
            status = False
            executed_notebook = os.getcwd() + "/" + notebook.split("/")[-1]
            with open(executed_notebook, mode="wt") as f:
                nbformat.write(nb, f)

        # check status
        self.assertTrue(status, "Notebook execution failed (%s)" % notebook)


@pytest.mark.filterwarnings("ignore:Session._key_changed is deprecated")
@pytest.mark.skipif(not kernel_found, reason=f"{kernel_name} kernel not found.")
class PipelineNotebookTest(NotebookTest):
    """Unit test notebook"""

    def test_basic_tutorial(self):
        self.run_notebook(resources.notebook("popmon_tutorial_basic.ipynb"))
