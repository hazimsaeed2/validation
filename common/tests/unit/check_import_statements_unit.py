import os
import traceback
import unittest

import xmlrunner


def imports(file_path):
    """
    Try to import the given file.

    Returns:
        (Bool, str)
        - True/False did the file import without exception
        - string error message if exception raised, else None
    """
    rel_path = os.path.splitext(file_path)[0]
    rel_path = rel_path.split("memberdna")[1]
    import_str = rel_path.replace("/", ".")
    import_str = "memberdna{}".format(import_str)

    try:
        __import__(import_str)
        return (True, None)
    except Exception:
        trace = traceback.format_exc()
        msg = "ERROR when importing {}: {}".format(import_str, trace)
        return (False, msg)


class TestImports(unittest.TestCase):
    """
    Collects all tests checking for successfull imports
    """

    def test_unit_imports(self):
        """
        Try importing all files matching the pattern */unittests/*_unit.py
        """
        rootdir = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )

        import_errors = 0
        for root, _, files in os.walk(rootdir):
            if root.endswith("unit"):
                for f in files:
                    if f.endswith("_unit.py"):
                        result = imports(os.path.join(root, f))
                        if not result[0]:
                            print(result[1])
                            import_errors += 1

        self.assertEqual(
            import_errors,
            0,
            "At least one unit test file has failing imports.",
        )


if __name__ == "__main__":

    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-test-reports"),
        # these make sure that some options that are not applicable
        # remain hidden from git merge origin/developthe help menu.
        failfast=False,
        buffer=False,
        catchbreak=False,
    )
