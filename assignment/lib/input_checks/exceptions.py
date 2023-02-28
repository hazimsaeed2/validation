"""
Classes to handle input specific exceptions.
"""


class AssignmentInputError(Exception):
    """Raised when the input to assignment is incorrect"""

    pass


class AssignmentInputWarning(Warning):
    """Raised when the input to assignment may be problematic."""

    pass
