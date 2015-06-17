#!/usr/bin/env python
import sh
import os
import sys

HOST = "http://unittest.jenkins.cloudera.com/job/verify-impala-toolchain-package-build/"
SOURCE = "http://github.mtv.cloudera.com/mgrund/impala-deps/raw/master"

OS_MAPPING = {
  "centos6" : "ec2-package-centos-6",
  "centos6.4" : "ec2-package-centos-6",
  "centos6.5" : "ec2-package-centos-6",
  "centos5" : "ec2-package-centos-5",
  "centos7" : "ec2-package-centos-7",
  "debian6" : "ec2-package-debian-6",
  "debian7" : "ec2-package-debian-7",
  "sles11" : "ec2-pacage-sles-11",
  "ubuntu12.04" : "ec2-package-ubuntu-12-04",
  "ubuntu14.04" : "ec2-package-ubuntu-14-04"
}

def map_release_label():
  """Gets the right package label from the OS version"""
  return OS_MAPPING["".join(map(lambda x: x.lower(), sh.lsb_release("-irs").split()))]


def download_package(name, destination, compiler=""):
  label = map_release_label()
  if len(compiler) > 0:
    compiler = "-" + compiler
  url = "{0}/label={1}/lastSuccessfulBuild/artifact/toolchain/build/{2}{3}.tar.gz".format(
    HOST, label, name, compiler)

  # Download the file
  print "Downloading {0}".format(name)
  sh.wget(url, directory_prefix=destination, no_clobber=True)
  # Extract
  print "Extracting {0}".format(name)
  sh.tar(z=True, x=True, f="{0}/{1}{2}.tar.gz".format(destination, name, compiler),
         directory=destination)
  sh.rm("{0}/{1}{2}.tar.gz".format(destination, name, compiler))


def bootstrap(packages, destination=None, compiler=None):
  """Bootstrap will create a bootstrap directory within $IMPALA_HOME and download and
  install the necessary packages and pre-built binaries."""
  path = os.getenv("IMPALA_HOME", None)
  if path is None:
    print "Impala environment not setup correctly, make sure $IMPALA_HOME is present."

  if destination is None:
    destination = os.path.join(path, "toolchain", "build")

  if compiler is None:
    compiler = "gcc-" + os.getenv("IMPALA_GCC_VERSION")

  # Create the destination directory if necessary
  if not os.path.exists(destination):
    os.makedirs(destination)

  for p in packages:
    download_package(p, destination, compiler)

if __name__ == "__main__":
  if len(sys.argv) < 3:
    print "usage bootstrap_toolchain.py compiler [package, package, ...]"
    sys.exit(1)

  compiler = sys.argv[1]
  packages = sys.argv[2:]
  bootstrap(packages, compiler=compiler)
