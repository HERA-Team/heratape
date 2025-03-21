# Copyright (c) 2025 HERA-Team
# Licensed under the 2-clause BSD License
"""Define version and namespace."""

import contextlib
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from setuptools_scm import get_version

from .files import Files
from .tapes import Tapes


# copy this function here from setup.py.
# Copying code is terrible, but it's better than altering the python path in setup.py.
def branch_scheme(version):  # pragma: nocover
    """
    Local version scheme that adds the branch name for absolute reproducibility.

    If and when this is added to setuptools_scm this function can be removed.
    """
    if version.exact or version.node is None:
        return version.format_choice("", "+d{time:{time_format}}", time_format="%Y%m%d")
    else:
        if version.branch == "main":
            return version.format_choice("+{node}", "+{node}.dirty")
        else:
            return version.format_choice("+{node}.{branch}", "+{node}.{branch}.dirty")


try:
    # get accurate version for developer installs
    version_str = get_version(
        Path(__file__).parent.parent.parent, local_scheme=branch_scheme
    )

    __version__ = version_str

except (LookupError, ImportError):
    with contextlib.suppress(PackageNotFoundError):
        # Set the version automatically from the package details.
        __version__ = version("heratape")

__all__ = [Files, Tapes]
