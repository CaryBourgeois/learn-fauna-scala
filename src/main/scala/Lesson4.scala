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
package com.fauna.learn

/*
 * These imports are for basic functionality around logging and JSON handling and Futures.
 * They should best be thought of as convenience items for our demo apps.
 */
import grizzled.slf4j.Logging

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Success, Try}
import util.Random._

/*
 * These are the required imports for Fauna.
 *
 * For these examples we are using the 2.1.0 version of the JVM driver. Also notice that we aliasing
 * the query and values part of the API to make it more obvious we we are using Fauna functionality.
 *
 */
import faunadb.{FaunaClient, query => q, values => v}

object Lesson4 extends App with Logging {
  import ExecutionContext.Implicits._

  case class Customer(id: Int, balance: Int)
  implicit val customerCodec = v.Codec.caseClass[Customer]
  case class Transaction(uuid: String, sourceCust: Int, destCust: Int, amount: Int)
  implicit val transactionCodec = v.Codec.caseClass[Transaction]

  def createDatabase(dcURL: String, secret: String, dbName: String): String = {
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

  def createDBClient(dcURL: String, secret: String): FaunaClient = {
    /*
    * Create the DB specific DB client using the DB specific key just created.
    */
    val client = FaunaClient(endpoint = dcURL, secret = secret)
    logger.info("Connected to FaunaDB as server")

    return client
  }

  def createClasses(client: FaunaClient): Unit = {
    /*
     * Create an class to hold customers
     */
    val result = client.query(
      q.Arr(
        q.CreateClass(q.Obj("name" -> "customers")),
        q.CreateClass(q.Obj("name" -> "transactions"))
      )
    )
    Await.result(result, Duration.Inf)
    logger.info(s"Created 'customer' class :: \n${JsonUtil.toJson(result)}")
  }

  def createIndices(client: FaunaClient): Unit = {
    /*
     * Create two indexes here. The first index is to query customers when you know specific id's.
     * The second is used to query customers by range. Examples of each type of query are presented
     * below.
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
        ),
        q.CreateIndex(
          q.Obj(
            "name" -> "customer_id_filter",
            "source" -> q.Class("customers"),
            "unique" -> true,
            "values" -> q.Arr(
              q.Obj("field" -> q.Arr("data", "id")),
              q.Obj("field" -> q.Arr("ref"))
            )
          )
        ),
        q.CreateIndex(
          q.Obj(
            "name" -> "transaction_uuid_filter",
            "source" -> q.Class("transactions"),
            "unique" -> true,
            "values" -> q.Arr(
              q.Obj("field" -> q.Arr("data", "id")),
              q.Obj("field" -> q.Arr("ref"))
            )
          )
        )
      )
    )
    Await.result(result2, Duration.Inf)
    logger.info(s"Created 'customer_by_id' index & 'customer_id_filter' index ::\n${JsonUtil.toJson(result2)}")
  }

  def createCustomers(client: FaunaClient, numCustomers: Int, initBalance: Int): Seq[v.RefV] = {
    /*
     * Create 'numCustomers' customer records with ids from 1 to 'numCustomers'
     */
    var custRefs: Seq[v.RefV] = null

    val result = Try{
      val stmt = client.query(
        q.Map(for (i <- 1 to numCustomers) yield {Customer(id = i, balance = initBalance)},
          q.Lambda { customer =>
            q.Create(
              q.Class("customers"),
              q.Obj("data" -> customer)
            )
          }
        )
      )
      Await.result(stmt, Duration.Inf)
    } match {
      case Success(s) => {
        custRefs = s.collect(v.Field("ref").to[v.RefV]).get
      }
    }

    logger.info(s"Created ${numCustomers} new customers with balance: ${initBalance}")

    return custRefs
  }

  def sumCustBalances(client: FaunaClient, custRefs: Seq[v.RefV]): Int = {
    /*
     * This is going to take the customer references that were created during the
     * createCustomers routine and aggregate all the balances for them. We could so this,
     * and probably would, with class index. In this case we want to take this approach to show
     * how to use references.
     */
    var balanceSum: Int = 0

    val result = Try{
      val stmt = client.query(
        q.Map(custRefs,
          q.Lambda { cust => q.Select("data", q.Get(cust))}
        )
      )
      Await.result(stmt, Duration.Inf)
    } match {
      case Success(s) => {
        val data = s.to[Seq[Customer]].get
        for ( c <- data) {
          balanceSum += c.balance
        }
      }
    }

    logger.info(s"Customer Balance Sum: ${balanceSum}")

    return balanceSum
  }

  def createTxn (client: FaunaClient, numCustomers: Int, maxTxnAmount: Int): Unit = {
    /*
     * This method is going to create a random transaction that moves a random amount
     * from a source customer to a destination customer. Prior to committing the transaction
     * a check will be performed to insure that the source customer has a sufficient balance
     * to cover the amount and not go into an overdrawn state.
     */
    val uuid: String = java.util.UUID.randomUUID.toString

    val sourceID = nextInt(numCustomers) + 1
    var destID = nextInt(numCustomers) + 1
    while (destID == sourceID) {
      destID = nextInt(numCustomers) + 1
    }
    val amount = nextInt(maxTxnAmount) + 1

    val transaction = Transaction(uuid, sourceID, destID, amount)

    val stmt = client.query(
      q.Let {
        val sourceCust = q.Get(q.Match(q.Index("customer_by_id"), sourceID))
        val destCust = q.Get(q.Match(q.Index("customer_by_id"), destID))
        q.Let {
          val sourceBalance = q.Select(q.Arr("data", "balance"), sourceCust)
          val destBalance = q.Select(q.Arr("data", "balance"), destCust)
          q.Let {
            val newSourceBalance = q.Subtract(q.Var("sourceBalance"), amount)
            val newDestBalance = q.Add(q.Var("destBalance"), amount)
            q.If(
              q.GTE(q.Var("newSourceBalance"), 0),
              q.Do(
                q.Create(q.Class("transactions"),
                  q.Obj("data" -> transaction)),
                q.Update(
                  q.Select("ref", sourceCust), q.Obj(
                    "data" -> q.Obj(
                      "balance" -> q.Var("newSourceBalance")))),
                q.Update(
                  q.Select("ref", destCust), q.Obj(
                    "data" -> q.Obj(
                      "balance" -> q.Var("newDestBalance"))))
              ),
              "Error. Insufficient funds."
            )
          }
        }
      }
    )
    Await.result(stmt, Duration.Inf)  // don't really need to do this here as we can send these all Async
  }

  val dcURL = "http://127.0.0.1:8443"
  val secret = "secret"
  val dbName = "LedgerExample"

  val dbSecret = createDatabase(dcURL, secret, dbName)

  val client = createDBClient(dcURL, dbSecret)

  createClasses(client)

  createIndices(client)

  val custRefs = createCustomers(client, 50, 100)

  sumCustBalances(client, custRefs)

  for (i <- 1 to 100) {
    createTxn(client, 50, 10)
  }

  sumCustBalances(client, custRefs)

  /*
   * Just to keep things neat and tidy, close the client connections
   */
  client.close()
  logger.info(s"Disconnected from FaunaDB as server for DB ${dbName}!")

  // add this at the end of execution to make things shut down nicely
  System.exit(0)
}
