import unittest
import unittest.mock as mock
import xmlrunner

from unittest import mock
from pe_member_dna.pipelines.lib.iotools import (
    s3_copy_version,
    s3_copy,
    copy_file_to_s3,
    copy_dir_to_s3,
)


class S3CopyVersionTestCase(unittest.TestCase):
    """
    Test that the s3 copy function copies only keys and can accept key version.
    """

    @mock.patch("memberdna.pipelines.lib.iotools.is_s3_file")
    def test_copy_non_file(self, is_s3_file):
        is_s3_file.return_value = False

        self.assertRaisesRegex(
            Exception,
            "Can only copy files\(s3 keys\)\.",
            s3_copy_version,
            "test_bucket",
            "source_key",
            "dest_key",
        )

    @mock.patch("memberdna.pipelines.lib.iotools.boto3")
    @mock.patch("memberdna.pipelines.lib.iotools.is_s3_file")
    def test_copy_current(self, is_s3_file, boto3):
        is_s3_file.return_value = True
        s3 = mock.MagicMock()
        s3.copy_object.return_value = None
        boto3.client.return_value = s3

        s3_copy_version("test_bucket", "source_key", "dest_key")

        s3.copy_object.assert_called_with(
            Bucket="test_bucket",
            Key="dest_key",
            CopySource={"Bucket": "test_bucket", "Key": "source_key"},
        )

    @mock.patch("memberdna.pipelines.lib.iotools.boto3")
    @mock.patch("memberdna.pipelines.lib.iotools.is_s3_file")
    def test_copy_version(self, is_s3_file, boto3):
        is_s3_file.return_value = True
        s3 = mock.MagicMock()
        s3.copy_object.return_value = None
        boto3.client.return_value = s3

        s3_copy_version(
            "test_bucket",
            "source_key",
            "dest_key",
            "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo",
        )

        s3.copy_object.assert_called_with(
            Bucket="test_bucket",
            Key="dest_key",
            CopySource={
                "Bucket": "test_bucket",
                "Key": "source_key",
                "VersionId": "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40"
                "Nr8X8gdRQBpUMLUo",
            },
        )


class S3CopyTestCase(unittest.TestCase):
    """
    Test that the s3 copy function copies both files and dirs.
    """

    @mock.patch("memberdna.pipelines.lib.iotools.s3_copy_version")
    @mock.patch("memberdna.pipelines.lib.iotools.is_s3_file")
    def test_copy_file(self, is_s3_file, s3_copy_version):
        is_s3_file.return_value = True
        s3_copy_version.return_value = None

        s3_copy("test_bucket", "source_key", "dest_key")

        s3_copy_version.assert_called_with(
            "test_bucket", "source_key", "dest_key"
        )

    @mock.patch("memberdna.pipelines.lib.iotools.list_s3_dir")
    @mock.patch("memberdna.pipelines.lib.iotools.s3_copy_version")
    @mock.patch("memberdna.pipelines.lib.iotools.is_s3_file")
    def test_copy_dir(self, is_s3_file, s3_copy_version, list_s3_dir):
        is_s3_file.side_effect = [False, True, True]
        list_s3_dir.return_value = [
            "s3://test_bucket/dir/key1",
            "s3://test_bucket/dir/key2",
        ]
        s3_copy_version.return_value = None

        s3_copy("test_bucket", "dir", "dest_dir")

        s3_copy_version.assert_has_calls(
            [
                mock.call("test_bucket", "dir/key1", "dest_dir/key1"),
                mock.call("test_bucket", "dir/key2", "dest_dir/key2"),
            ]
        )


class CopyFileToS3TestCase(unittest.TestCase):
    """
    Test that the copy function copies files.
    """

    @mock.patch("memberdna.pipelines.lib.iotools.write_text_to_s3")
    def test_copy_file(self, write_text_to_s3):
        write_text_to_s3.return_value = None

        copy_file_to_s3(
            "unittests/test_data/iotools/file.txt",
            "s3://test_bucket/test/file.txt",
        )

        write_text_to_s3.assert_called_with(
            "test_bucket", "test/file.txt", b"test"
        )


class CopyDirToS3TestCase(unittest.TestCase):
    """
    Test that the copy function copies dirs recursively.
    """

    def test_copy_nondir(self):
        self.assertRaisesRegex(
            Exception,
            "Dir dummy_dir does not exist",
            copy_dir_to_s3,
            "dummy_dir",
            "s3://test_bucket/dummy_dir",
        )

    @mock.patch("memberdna.pipelines.lib.iotools.copy_file_to_s3")
    def test_copy_dir(self, copy_file_to_s3):
        copy_file_to_s3.return_value = None
        copy_dir_to_s3(
            "unittests/test_data/iotools", "s3://test_bucket/iotools"
        )

        calls = [
            mock.call(
                "unittests/test_data/iotools/test_dir/test_dir/file.txt",
                "s3://test_bucket/iotools/test_dir/test_dir/file.txt",
            ),
            mock.call(
                "unittests/test_data/iotools/test_dir/file.txt",
                "s3://test_bucket/iotools/test_dir/file.txt",
            ),
            mock.call(
                "unittests/test_data/iotools/file.txt",
                "s3://test_bucket/iotools/file.txt",
            ),
        ]

        copy_file_to_s3.assert_has_calls(calls)


if __name__ == "__main__":
    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-test-reports"),
        # these make sure that some options that are not applicable
        # remain hidden from the help menu.
        failfast=False,
        buffer=False,
        catchbreak=False,
    )
