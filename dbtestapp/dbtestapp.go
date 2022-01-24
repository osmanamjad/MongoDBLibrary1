// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0
// SPDX-License-Identifier: LicenseRef-ONF-Member-Only-1.0

package main

import (
	"log"
	"time"
	//"context"
	//"fmt"
	//"os"

	//"go.mongodb.org/mongo-driver/bson/primitive"
	//"go.mongodb.org/mongo-driver/mongo"
	//"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/omec-project/MongoDBLibrary"
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

	insertStudentInDB("Osman Amjad", 21)

	insertStudentInDB("John Smith", 25)

	student := getStudentFromDB("Osman Amjad", 21)
	log.Println("Printing student")
	log.Println(student)
	log.Println(student.Name)
	log.Println(student.Age)
	log.Println(student.CreatedAt)

	createDocumentWithTimeout()

	uniqueId := MongoDBLibrary.RestfulAPIGetUniqueIdentity()
	log.Println(uniqueId)

	uniqueId = MongoDBLibrary.RestfulAPIGetUniqueIdentity()
	log.Println(uniqueId)

	uniqueId = MongoDBLibrary.RestfulAPIGetUniqueIdentity()
	log.Println(uniqueId)

	for {
		time.Sleep(100 * time.Second)
	}
}

func getStudentFromDB(name string, age int) Student {
	filter := bson.M{}
	filter["name"] = name
	filter["age"] = age

	result := MongoDBLibrary.RestfulAPIGetOneCustomDataStructure("student", filter)
	bsonBytes, _ := bson.Marshal(result)

	var student Student
	bson.Unmarshal(bsonBytes, &student)

	return student
}

func insertStudentInDB(name string, age int) {
	student := Student {
		Name: name,
		Age: age,
		CreatedAt: time.Now(),
	}
	filter := bson.M{}
	MongoDBLibrary.RestfulAPIPutOneCustomDataStructure("student", filter, student)
}

func createDocumentWithTimeout() {
	putData := bson.M{}
	putData["name"] = "Yak"
	putData["createdAt"] = time.Now()
	filter := bson.M{}
	MongoDBLibrary.RestfulAPIPutOneWithTimeout("timeout", filter, putData, 120, "createdAt")
}
