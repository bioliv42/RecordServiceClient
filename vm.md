---
layout: page
title: 'Download and Install RecordService VM'
---

## Download RecordService VM

These are the steps to download the RecordService VM.

1. Install VirtualBox. The VM has been tested to work with VirtualBox version 4.3 on Ubuntu 14.04 and VirtualBox version 5 on OSX 10.9. Download VirtualBox for free at [https://www.virtualbox.org/wiki/Downloads](https://www.virtualbox.org/wiki/Downloads).
1. Download the VM from **NEED CORRECT URL**.	 
1. Untar/zip the bundle. It should contain two scripts, `install.sh` and `vmEnv.sh`.

## Install RecordService VM
These are the steps to install the RecordService VM.

1. In a terminal window, run the script install.sh.<br/><br/>
This script downloads an `ova` file and load it into VirtualBox. The script might ask you to enter a password because it edits your /etc/hosts file to give the vm a stable ip address, quickstart.cloudera. When the script completes, you  have a running virtual machine that functions as a RecordService server. To test that the VM is running and IP forwarding has been successfully configured, try ssh-ing to the machine with the command `ssh cloudera@quickstart.cloudera`. The password is `cloudera`. If you are unable to ssh to the VM, refer to  [Troubleshooting the VM Configuration](#troubleshooting-the-vm-configuration).
1. Navigate to the root of your RecordServiceClient repository, $RECORD_SERVICE_HOME.
1. Run `source config.sh`.
1. Run `source vm_env.sh`.
If your client code is compiled, your environment is not configured to run the tests on the VM. Use the following command line instructions to run all JUnit tests.

```
cd $RECORD_SERVICE_HOME/java`
mvn test
```

## Troubleshooting the VM configuration

### Unable to ssh to the VM
* Ensure that the ssh daemon is running on your machine.
* Ensure that the RecordService VM is running. In your terminal, enter the command: 
```
VBoxManage list runningvms
```
You should see “rs-demo” listed as a running VM.

## Verify VM is properly listed in /etc/hosts

Check that the VM is properly listed in your /etc/hosts file. If you open that file you should see a line that lists an IP followed by quickstart.cloudera. You can check the VM’s IP with the following command:
```
VBoxManage guestproperty get rs-demo /VirtualBox/GuestInfo/Net/0/V4/IP
```

## Verify Known Hosts

If you’ve used a Cloudera QuickStart VM before, it’s possible that your known hosts file already has an entry for quickstart.cloudera registered to a different key. Delete any reference to quickstart.cloudera from your known hosts file, usually found in ~/.ssh/known_hosts.
