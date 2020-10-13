#!/bin/bash

# required for SELinux to allow Docker to work
# we're prompting the user here (for password) so that the run script can proceed once the build is completed
sudo chcon -Rt svirt_sandbox_file_t .

mvn assembly:assembly -DskipTests
