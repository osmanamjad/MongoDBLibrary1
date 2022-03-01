<!--
SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
Copyright 2019 free5GC.org
SPDX-License-Identifier: Apache-2.0
-->
# MongoDBLibrary

APIs to access mongoDB are defined in this repository.

## testApp

Testapp in this repository builds a container of test application. All the APIs in the 
MongoDBLibrary are used by testApp. All new developement of MongoDBlibrary comes with 
correspondinig example in the testApp. 

You can use AIAB ( Aether in a Box ) setup to run test application. Refer [SD-Core
document](https://docs.sd-core.opennetworking.org/master/developer/aiab.html#)

## Upcoming Work

1. Provide More APIs to assign unique resources when multiple instances of Network Functions Supported
2. APIs to lock database so that no 2 instances update the same database content. 
3. Add more APIs which will help cloud native application development.
4. Deploy MongoDB with sharding.
