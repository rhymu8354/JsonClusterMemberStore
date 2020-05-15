# JsonClusterMemberStore

**⚠ NOTICE ⚠ This repository has been abandoned** -- The maintainer has reached the
conclusion that the original database abstraction was not appropriate for their
larger project, and even with the redesigned database abstraction, the benefits
of getting it adapted to JSON would not be worth the effort.

This library is intended for use in systems where data is stored for exlusive
use by a member of a larger cluster (such as in the Raft Consensus Algorithm).
It is used to separate the application from the implementation of how it stores
its data on a relational database.

This is a specific implementation which stores data as a composite JSON object.

## Usage

The `ClusterMemberStore::JsonDatabase` class an implementation of the
`ClusterMemberStore::Database` pure abstract interface class.  It adapts the
interface to fit a JSON file.  The `Open` method reads and parses the JSON
file, making the data within available via the `Database` interface.  The
`Commit` method is used to ensure all changes are written back out as JSON to
the file.

## Supported platforms / recommended toolchains

This is a portable C++11 library which depends only on the C++11 compiler and
standard library, so it should be supported on almost any platform.  The
following are recommended toolchains for popular platforms.

* Windows -- [Visual Studio](https://www.visualstudio.com/) (Microsoft Visual
  C++)
* Linux -- clang or gcc
* MacOS -- Xcode (clang)

## Building

This library is not intended to stand alone.  It is intended to be included in
a larger solution which uses [CMake](https://cmake.org/) to generate the build
system and build applications which will link with the library.

There are two distinct steps in the build process:

1. Generation of the build system, using CMake
2. Compiling, linking, etc., using CMake-compatible toolchain

### Prerequisites

* [CMake](https://cmake.org/) version 3.8 or newer
* C++11 toolchain compatible with CMake for your development platform (e.g.
  [Visual Studio](https://www.visualstudio.com/) on Windows)
* [ClusterMemberStore](https://github.com/rhymu8354/ClusterMemberStore.git) -
  the library which defines the abstract interface implemented by this project
* [Json](https://github.com/rhymu8354/Json.git) - a library which implements
  [RFC 7159](https://tools.ietf.org/html/rfc7159), "The JavaScript Object
  Notation (JSON) Data Interchange Format".
* [SystemAbstractions](https://github.com/rhymu8354/SystemAbstractions.git) - a
  cross-platform adapter library for system services whose APIs vary from one
  operating system to another

### Build system generation

Generate the build system using [CMake](https://cmake.org/) from the solution
root.  For example:

```bash
mkdir build
cd build
cmake -G "Visual Studio 15 2017" -A "x64" ..
```

### Compiling, linking, et cetera

Either use [CMake](https://cmake.org/) or your toolchain's IDE to build.
For [CMake](https://cmake.org/):

```bash
cd build
cmake --build . --config Release
```
