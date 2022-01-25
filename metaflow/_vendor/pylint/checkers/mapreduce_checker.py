# Copyright (c) 2020 Frank Harrison <frank@doublethefish.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE
import abc


class MapReduceMixin(metaclass=abc.ABCMeta):
    """A mixin design to allow multiprocess/threaded runs of a Checker"""

    @abc.abstractmethod
    def get_map_data(self):
        """Returns mergable/reducible data that will be examined"""

    @classmethod
    @abc.abstractmethod
    def reduce_map_data(cls, linter, data):
        """For a given Checker, receives data for all mapped runs"""
