#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# This bash script aims to predownload dependency tarballs for the E2E tests.
# It aims to test the blah blah blah.


if [ -z "$E2E_TARBALL_CACHE" ] ; then
    echo "You have to export the E2E Tarball Cache as E2E_TARBALL_CACHE"
    exit 1
fi

mkdir -p $E2E_TARBALL_CACHE

# Given a variable name and a URL, checks whether the file exists in the E2E_TARBALL_CACHE,
# otherwise retrieves from source, and evals the path to the result variable.
# For example:
# get_artifact result https://archive.apache.org/artifact.tar.gz
# echo $result

function get_artifact {
    local __resultvar=$1
    BASENAME="`basename $2`"
    echo "Checking $2"
    if [ -f "$E2E_TARBALL_CACHE/$BASENAME" ]; then
        echo "$BASENAME already exists. Retrieving from local cache"
    else
        echo "$BASENAME does not exist. Retrieving from $2"
        curl $2 --retry 10 --retry-max-time 120 --output $E2E_TARBALL_CACHE/$BASENAME
    fi
    eval $__resultvar="'$E2E_TARBALL_CACHE/$BASENAME'"
}
