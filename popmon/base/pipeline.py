import logging
from ..base import Module


class Pipeline(Module):
    """Base class used for to run modules in a pipeline.
    """
    def __init__(self, modules, logger=None):
        """Initialization of the pipeline

        :param list modules: modules of the pipeline.
        :param logger: logger to be used by each module.
        """
        super().__init__()
        self.modules = modules
        self.set_logger(logger)

    def set_logger(self, logger):
        """Set the logger to be used by each module

        :param logger: input logger
        """
        self.logger = logger
        if self.logger is None:
            self.logger = logging.getLogger()
        for module in self.modules:
            module.set_logger(self.logger)

    def add_modules(self, modules):
        """Add more modules to existing list of modules.

        :param list modules: list of more modules
        """
        if len(modules):
            self.modules.extend(modules)

    def transform(self, datastore):
        """Central function of the pipeline.

        Calls transform() of each module in the pipeline.
        Typically transform() of a module takes something from the datastore, does something to it,
        and puts the results back into the datastore again, to be passed on to the next module in the pipeline.

        :param dict datastore: input datastore
        :return: updated output datastore
        :rtype: dict
        """

        for module in self.modules:
            datastore = module.transform(datastore)
        return datastore
