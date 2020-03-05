MANUAL
======
[![Build Status](https://travis-ci.org/DANS-KNAW/easy-update-fs-rdb.png?branch=master)](https://travis-ci.org/DANS-KNAW/easy-update-fs-rdb)

Update the EASY File-system RDB with file and folder metadata from Fedora.


SYNOPSIS
--------

    easy-update-fs-rdb [--file|-f <text-file-with-dataset-id-per-line> | --dataset-pids|-d <dataset-pid>]


DESCRIPTION
-----------

The EASY File-system RDB (Relational Database) is a component that stores basis metadata about files and folders in 
the EASY Fedora Commons Repository. Its purpose is to provide quick access to this metadata, which is necessary for
the EASY Web-UI to function properly. ``easy-update-fs-rdb`` extracts the required file and folder metadata from
Fedora and updates the File-system RDB with this metadata. 

When an error occurs during the insert/update of the db-records for a dataset, the database transaction for that dataset is 'rolled back'
and executions stops. Note that in this case any remaining datasets (of the batch) still need to be processed!


ARGUMENTS
---------

     -d, --dataset-pids  <arg>...   ids of datasets for which to update the file and folder metadata in the
                                    File-system RDB
     -f, --file  <arg>              Text file with a dataset-id per line
     -h, --help                     Show help message
     -v, --version                  Show version of this program

    trailing arguments:
     dataset-pids (not required)   ids of datasets for which to update the file and folder metadata in the
                                   File-system RDB


INSTALLATION AND CONFIGURATION
------------------------------
Currently this project is built as an RPM package for RHEL7/CentOS7 and later. The RPM will install the binaries to
`/opt/dans.knaw.nl/easy-update-fs-rdb` and the configuration files to `/etc/opt/dans.knaw.nl/easy-update-fs-rdb`. 

To install the module on systems that do not support RPM, you can copy and unarchive the tarball to the target host.
You will have to take care of placing the files in the correct locations for your system yourself. For instructions
on building the tarball, see next section.


BUILDING FROM SOURCE
--------------------
Prerequisites:

* Java 8 or higher
* Maven 3.3.3 or higher
* RPM

Steps:
    
    git clone https://github.com/DANS-KNAW/easy-update-fs-rdb.git
    cd easy-update-fs-rdb 
    mvn clean install

If the `rpm` executable is found at `/usr/local/bin/rpm`, the build profile that includes the RPM 
packaging will be activated. If `rpm` is available, but at a different path, then activate it by using
Maven's `-P` switch: `mvn -Pprm install`.

Alternatively, to build the tarball execute:

    mvn clean install assembly:single
