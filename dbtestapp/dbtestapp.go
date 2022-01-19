// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0
// SPDX-License-Identifier: LicenseRef-ONF-Member-Only-1.0

package main

import (
	"log"
	"time"
	"context"
	"fmt"
	"os"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/osmanamjad/MongoDBLibrary"
	"go.mongodb.org/mongo-driver/bson"
)

type Student struct {
	//ID     		primitive.ObjectID 	`bson:"_id,omitempty"`
	Name      	string				`bson:"name,omitempty"`
	Age 	  	int 				`bson:"age,omitempty"`
	CreatedAt 	time.Time			`bson:"createdAt,omitempty"`
}

func main() {
	log.Println("dbtestapp started")

	// connect to mongoDB
	MongoDBLibrary.SetMongoDB("free5gc", "mongodb://mongodb:27017")

	insertStudentInDB()

	createStudentWithTimeout()

	uniqueId := getUniqueIdentity("simapp")
	log.Println(uniqueId)

	uniqueId = getUniqueIdentity("SMF")
	log.Println(uniqueId)

	uniqueId = getUniqueIdentity("UDF")
	log.Println(uniqueId)

	for {
		time.Sleep(100 * time.Second)
	}
}

func insertStudentInDB() {
	student := Student {
		Name: "Osman Amjad",
		Age: 21,
		CreatedAt: time.Now(),
	}
	filter = bson.M{}
	MongoDBLibrary.RestfulAPIPutOneCustomDataStructure("student", filter, student)
}

func getUniqueIdentity(name string) int32 {
	putData := bson.M{}
	putData["name"] = name
	filter := bson.M{}
	return MongoDBLibrary.RestfulAPIGetUniqueIdentity("uniqueIds", filter, putData)
}

func createStudentWithTimeout() {
	putData := bson.M{}
	putData["name"] = "John"
	putData["createdAt"] = time.Now()
	filter := bson.M{}
	MongoDBLibrary.RestfulAPIPutOneWithTimeout("student", filter, putData, 120, "createdAt")
}
