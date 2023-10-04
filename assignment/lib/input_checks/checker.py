"""
Helper classes and functions for dealing with assignment input checks.
"""

from pe_memberdna.lib.misc import print_table


class Check:
    """
    Represents a check on input.

    Attributes:
        name (str): name of the check being performed
        file (str): name of file being checked
        status (bool): result of the check
        details (str): detail of the check/status
    """

    def __init__(self, name, file, status=None, details=None):
        self.name = name
        self.file = file
        self.set_status(status, details)

    def set_status(self, status, details=None):
        """Set a status and corresponding details"""
        self.status = status
        self.details = details

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.file == other.file
            and self.status == other.status
            and self.details == other.details
        )

    def __repr__(self):
        return f"[{self.name}, {self.file}, {self.status}]"


def summarize(checks):
    """
    Convert list of check objects to a list of lists.

    Parameters:
        checks (list): list of check objects

    Returns:
        [[]] of check attributes
    """
    out = [["Pass?", "File", "Check", "Details"]]
    for check in checks:
        row = [check.status, check.file, check.name, check.details]
        out.append(row)
    return out


def print_summary(checks):
    """
    Print user-friendly summary of the checks to stdout.
    Parameters:
        checks (list): list of check objects

    Generates:
        stdout content
    """
    summary = summarize(checks)
    print("\n")
    print_table(summary)
