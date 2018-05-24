/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fauna.learnfauna

/*
 * These imports are for basic functionality around logging and JSON handling and Futures.
 * They should best be thought of as convenience items for our demo apps.
 */
import grizzled.slf4j.Logging

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

/*
 * These are the required imports for Fauna.
 *
 * For these examples we are using the 2.1.0 version of the JVM driver. Also notice that we aliasing
 * the query and values part of the API to make it more obvious we we are using Fauna functionality.
 *
 */
import faunadb.{FaunaClient, query => q, values => v}

object Lesson2 extends App with Logging {
  import ExecutionContext.Implicits._

  def createDatabase (dcURL: String, secret: String, dbName: String): String = {
    /*
     * Create an admin client. This is the client we will use to create the database.
     */
    val adminClient = FaunaClient(endpoint = dcURL, secret = secret)
    logger.info("Connected to FaunaDB as Admin!")

    /*
     * The code below creates the Database that will be used for this example. Please note that
     * the existence of the database is evaluated, deleted if it exists and recreated with a single
     * call to the Fauna DB.
     */
    var queryResponse = adminClient.query(
      q.Arr(
        q.If(
          q.Exists(q.Database(dbName)),
          q.Delete(q.Database(dbName)),
          true
        ),
        q.CreateDatabase(q.Obj("name" -> dbName))
      )
    )
    Await.result(queryResponse, Duration.Inf)
    logger.info(s"Created database: ${dbName} :: \n${JsonUtil.toJson(queryResponse)}")

    /*
     * Create a key specific to the database we just created. We will use this to
     * create a new client we will use in the remainder of the examples.
     */
    queryResponse = adminClient.query(
      q.CreateKey(q.Obj("database" -> q.Database(dbName), "role" -> "server"))
    )
    val key = Await.result(queryResponse, Duration.Inf)
    val dbSecret = key(v.Field("secret").to[String]).get
    logger.info(s"DB ${dbName} secret: ${dbSecret}")

    adminClient.close()
    logger.info("Disconnected from FaunaDB as Admin!")

    return dbSecret
  }

  def createDBClient (dcURL: String, secret: String): FaunaClient = {
    /*
    * Create the DB specific DB client using the DB specific key just created.
    */
    val client = FaunaClient(endpoint = dcURL, secret = secret)
    logger.info("Connected to FaunaDB as server")

    return client
  }

  def createSchema (client: FaunaClient): Unit = {
    /*
     * Create an class to hold customers
     */
    val result = client.query(
      q.CreateClass(q.Obj("name" -> "customers"))
    )
    Await.result(result, Duration.Inf)
    logger.info(s"Created customer class :: \n${JsonUtil.toJson(result)}")

    /*
     * Create the Indexes within the database. We will use these to access record in later lessons
     */
    val result2 = client.query(
      q.Arr(
        q.CreateIndex(
          q.Obj(
            "name" -> "customer_by_id",
            "source" -> q.Class("customers"),
            "unique" -> true,
            "terms" -> q.Arr(q.Obj("field" -> q.Arr("data", "id")))
          )
        )
      )
    )
    Await.result(result2, Duration.Inf)
    logger.info(s"Created customer_by_id index :: \n${JsonUtil.toJson(result2)}")
  }

  def createCustomer (client: FaunaClient, custID: Int, balance: Int): Unit = {
    /*
     * Create a customer (record)
     */
    val result = client.query(
      q.Create(
        q.Class("customers"), q.Obj("data" -> q.Obj("id" -> custID, "balance" -> balance))
      )
    )
    Await.result(result, Duration.Inf)
    logger.info(s"Create \'customer\' ${custID}: \n${JsonUtil.toJson(result)}")
  }

  def readCustomer (client: FaunaClient, custID: Int): Unit = {
    /*
     * Read the customer we just created
     */
    val result = client.query(
      q.Select("data", q.Get(q.Match(q.Index("customer_by_id"), custID)))
    )
    Await.result(result, Duration.Inf)
    logger.info(s"Read \'customer\' ${custID}: \n${JsonUtil.toJson(result)}")
  }

  def updateCustomer (client: FaunaClient, custID: Int, newBalance: Int): Unit = {
    /*
     * Update the customer
     */
    val result = client.query(
      q.Update(
        q.Select("ref", q.Get(q.Match(q.Index("customer_by_id"), custID))),
        q.Obj("data" -> q.Obj("balance" -> newBalance)
        )
      )
    )
    Await.result(result, Duration.Inf)
    logger.info(s"Update \'customer\' ${custID}: \n${JsonUtil.toJson(result)}")
  }

  def deleteCustomer (client: FaunaClient, custID: Int): Unit = {
    /*
     * Delete the customer
     */
    val result = client.query(
      q.Delete(
        q.Select("ref", q.Get(q.Match(q.Index("customer_by_id"), custID)))
      )
    )
    logger.info(s"Delete \'customer\' ${custID}: \n${JsonUtil.toJson(result)}")
  }


  val dcURL = "http://127.0.0.1:8443"
  val secret = "secret"
  val dbName = "LedgerExample"

  val dbSecret = createDatabase(dcURL, secret, dbName)

  val client = createDBClient(dcURL, dbSecret)

  createSchema(client)

  createCustomer(client, 0, 100)

  readCustomer(client, 0)

  updateCustomer(client, 0, 200)

  readCustomer(client, 0)

  deleteCustomer(client, 0)

  /*
   * Just to keep things neat and tidy, close the client connections
   */
  client.close()
  logger.info(s"Disconnected from FaunaDB as server for DB ${dbName}!")

  // add this at the end of execution to make things shut down nicely
  System.exit(0)
}

