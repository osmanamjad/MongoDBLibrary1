// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0
//

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
	Subject		string 				`bson:"subject,omitempty"`
	CreatedAt 	time.Time			`bson:"createdAt,omitempty"`
}

func main() {
	log.Println("dbtestapp started")

	// connect to mongoDB
	MongoDBLibrary.SetMongoDB("sdcore", "mongodb://mongodb:27017")

	// test inserting document with timeout
	TestDocumentWithTimeout()

	// test inserting and getting document that uses custom data structure
	TestCustomDataStructure()

	// test getting unique id, and getting unique id within a range
	TestGetUniqueIdentity()

	// test getting id from pool. 
	TestGetIdFromPool()

	// test getting chunk of ids from pool
	TestGetChunkFromPool()

	for {
		time.Sleep(100 * time.Second)
	}
}

func TestGetChunkFromPool() {
	log.Println("TESTING CHUNK APPROACH")

	randomId, lower, upper, err := MongoDBLibrary.GetChunkFromPool("studentIdsChunkApproach")
	log.Println(randomId, lower, upper)
	if (err != nil) {log.Println(err.Error())}

	MongoDBLibrary.InitializeChunkPool("studentIdsChunkApproach", 0, 1000, 5, 100) // min, max, retries, chunkSize

	randomId, lower, upper, err = MongoDBLibrary.GetChunkFromPool("studentIdsChunkApproach")
	log.Println(randomId, lower, upper)
	if (err != nil) {log.Println(err.Error())}

	randomId, lower, upper, err = MongoDBLibrary.GetChunkFromPool("studentIdsChunkApproach")
	log.Println(randomId, lower, upper)
	if (err != nil) {log.Println(err.Error())}

	MongoDBLibrary.ReleaseChunkToPool("studentIdsChunkApproach", randomId)
}

func TestGetIdFromPool() {
	log.Println("TESTING POOL OF IDS")

	MongoDBLibrary.InitializePool("pool1", 10, 32)
	
	uniqueId, err := MongoDBLibrary.GetIDFromPool("pool1")
	log.Println(uniqueId)

	MongoDBLibrary.ReleaseIDToPool("pool1", uniqueId)

	uniqueId, err = MongoDBLibrary.GetIDFromPool("pool1")
	log.Println(uniqueId)

	uniqueId, err = MongoDBLibrary.GetIDFromPool("pool1")
	log.Println(uniqueId)

	log.Println("TESTING INSERT APPROACH")
	var randomId int32

	randomId, err = MongoDBLibrary.GetIDFromInsertPool("insertApproach")
	log.Println(randomId)
	if (err != nil) {log.Println(err.Error())}

	MongoDBLibrary.InitializeInsertPool("insertApproach", 0, 1000, 3)

	randomId, err = MongoDBLibrary.GetIDFromInsertPool("insertApproach")
	log.Println(randomId)
	if (err != nil) {log.Println(err.Error())}

	randomId, err = MongoDBLibrary.GetIDFromInsertPool("insertApproach")
	log.Println(randomId)
	if (err != nil) {log.Println(err.Error())}

	MongoDBLibrary.ReleaseIDToInsertPool("insertApproach", randomId)

	log.Println("TESTING RETRIES")

	MongoDBLibrary.InitializeInsertPool("testRetry", 0, 6, 3)

	randomId, err = MongoDBLibrary.GetIDFromInsertPool("testRetry")
	log.Println(randomId)
	if (err != nil) {log.Println(err.Error())}

	randomId, err = MongoDBLibrary.GetIDFromInsertPool("testRetry")
	log.Println(randomId)
	if (err != nil) {log.Println(err.Error())}
}

func TestGetUniqueIdentity() {
	uniqueId := MongoDBLibrary.GetUniqueIdentity()
	log.Println(uniqueId)

	uniqueId = MongoDBLibrary.GetUniqueIdentity()
	log.Println(uniqueId)

	uniqueId = MongoDBLibrary.GetUniqueIdentityWithinRange(3, 6)
	log.Println(uniqueId)

	uniqueId = MongoDBLibrary.GetUniqueIdentityWithinRange(3, 6)
	log.Println(uniqueId)
}

func TestCustomDataStructure() {
	insertStudentInDB("Osman Amjad", 21)
	student, err := getStudentFromDB("Osman Amjad")
	if err == nil {
		log.Println("Printing student1")
		log.Println(student)
		log.Println(student.Name)
		log.Println(student.Age)
		log.Println(student.CreatedAt)
	} else {
		log.Println("Error getting student: " + err.Error())
	}

	insertStudentInDB("John Smith", 25)

	// test student that doesn't exist.
	student, err = getStudentFromDB("Nerf Doodle")
	if err == nil {
		log.Println("Printing student2")
		log.Println(student)
		log.Println(student.Name)
		log.Println(student.Age)
		log.Println(student.CreatedAt)
	} else {
		log.Println("Error getting student: " + err.Error())
	}
}

func TestDocumentWithTimeout() {
	putData := bson.M{}
	putData["name"] = "Yak"
	putData["createdAt"] = time.Now()
	filter := bson.M{}
	MongoDBLibrary.PutOneWithTimeout("timeout", filter, putData, 120, "createdAt")
}

func getStudentFromDB(name string) (Student, error) {
	var student Student
	filter := bson.M{}
	filter["name"] = name

	result, err := MongoDBLibrary.GetOneCustomDataStructure("student", filter)

	if err == nil {
		bsonBytes, _ := bson.Marshal(result)
		bson.Unmarshal(bsonBytes, &student)

		return student, nil
	}
	return student, err
}

func insertStudentInDB(name string, age int) {
	student := Student{
		Name:      name,
		Age:       age,
		CreatedAt: time.Now(),
	}
	filter := bson.M{}
	MongoDBLibrary.PutOneCustomDataStructure("student", filter, student)
}