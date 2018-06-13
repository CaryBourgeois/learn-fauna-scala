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

/*
 * These are the required imports for Fauna.
 *
 * For these examples we are using the 2.2.0 version of the JVM driver. Also notice that we aliasing
 * the query and values part of the API to make it more obvious we we are using Fauna functionality.
 *
 */
import faunadb.{FaunaClient, query => q, values => v}

object Lesson3 extends App with Logging {
  import ExecutionContext.Implicits._

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

  def createSchema(client: FaunaClient): Unit = {
    /*
     * Create an class to hold customers
     */
    val result = client.query(
      q.CreateClass(q.Obj("name" -> "customers"))
    )
    Await.result(result, Duration.Inf)
    logger.info(s"Created 'customer' class :: \n${JsonUtil.toJson(result)}")

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
        )
      )
    )
    Await.result(result2, Duration.Inf)
    logger.info(s"Created 'customer_by_id' index & 'customer_id_filter' index ::\n${JsonUtil.toJson(result2)}")
  }

  def createCustomers(client: FaunaClient): Unit = {
    /*
     * Create numCustomers customer records with ids from 1 to 20
     */
    val result = client.query(
      q.Map((1 to 20).toList,
        q.Lambda { id =>
          q.Create(
            q.Class("customers"),
            q.Obj("data" -> q.Obj("id" -> id, "balance" -> 100))
          )
        }
      )
    )
    Await.result(result, Duration.Inf)
  }

  def readCustomer(client: FaunaClient, custID: Int): Unit = {
    /*
     * Read the customer we just created
     */
    val result = client.query(
      q.Select("data", q.Get(q.Match(q.Index("customer_by_id"), custID)))
    )
    Await.result(result, Duration.Inf)
    logger.info(s"Read \'customer\' ${custID}: \n${JsonUtil.toJson(result)}")
  }

  def readThreeCustomers(client: FaunaClient, custID1: Int, custID2: Int, custID3: Int): Unit = {
    /*
     * Here is a more general use case where we retrieve multiple class references
     * by id and return the actual data underlying them.
     */
    val result = client.query(
      q.Map(
        q.Paginate(
          q.Union(
            q.Match(q.Index("customer_by_id"), custID1),
            q.Match(q.Index("customer_by_id"), custID2),
            q.Match(q.Index("customer_by_id"), custID3)
          )
        ),
        q.Lambda { x => q.Select("data", q.Get(x)) }
      )
    )
    Await.result(result, Duration.Inf)
    logger.info(s"Union specific \'customer\' ${custID1}, ${custID2}, ${custID3} \n${JsonUtil.toJson(result)}")
  }

  def readListOfCustomers(client: FaunaClient, custIDs: List[Int]): Unit = {
    /*
     * Finally a much more general use case where we can supply any number of id values
     * and return the data for each.
     */
    val result = client.query(
      q.Map(
        q.Paginate(
          q.Union(
            q.Map(custIDs,
              q.Lambda { y => q.Match(q.Index("customer_by_id"), y) }
            )
          )
        ),
        q.Lambda { x => q.Select("data", q.Get(x)) }
      )
    )
    Await.result(result, Duration.Inf)
    logger.info(s"Union variable \'customer\' ${custIDs}: \n${JsonUtil.toJson(result)}")
  }

  def readCustomersLessThan(client: FaunaClient, maxCustID: Int): Unit = {
    /*
     * In this example we use the values based filter 'customer_id_filter'.
     * using this filter we can query by range. This is an example of returning
     * all the values less than(<) or before 5. The keyword 'after' can replace
     * 'before' to yield the expected results.
     */
    val result = client.query(
      q.Map(
        q.Paginate(q.Match(q.Index("customer_id_filter")), cursor = q.Before(5)),
        q.Lambda { x => q.Select("data", q.Get(q.Select(1, x))) }
      )
    )
    Await.result(result, Duration.Inf)
    logger.info(s"Query for id\'s < ${maxCustID} : \n${JsonUtil.toJson(result)}")
  }

  def readCustomersBetween(client: FaunaClient, minCustID: Int, maxCustID: Int): Unit = {
    /*
     * Extending the previous example to show getting a range between two values.
     */
    val result = client.query(
      q.Map(
        q.Filter(q.Paginate(q.Match(q.Index("customer_id_filter")), cursor = q.Before(maxCustID)),
          q.Lambda { y => q.LTE(minCustID, q.Select(0, y)) } ),
        q.Lambda { x => q.Select("data", q.Get(q.Select(1, x))) }
      )
    )
    Await.result(result, Duration.Inf)
    logger.info(s"Query for id\'s >= ${minCustID}  and < ${maxCustID} : \n${JsonUtil.toJson(result)}")
  }

  def readAllCustomers(client: FaunaClient): Unit = {
    /*
     * Read all the records that we created.
     * Use a small'ish page size so that we can demonstrate a paging example.
     *
     * NOTE: after is inclusive of the value.
     */
    val pageSize: Int = 16
    var cursorPos: Option[v.Value] = None

    do {
      val result = Try {
        val stmt = client.query(
          q.Map(
            q.Paginate(q.Match(q.Index("customer_id_filter")),
              cursor = cursorPos.map(q.After(_)) getOrElse q.NoCursor,
              size = pageSize),
            q.Lambda { x => q.Select("data", q.Get(q.Select(1, x))) }
          )
        )
        Await.result(stmt, Duration.Inf)
      } match {
        case Success(s) => {
          val data = s("data").to[List[v.Value]].get
          logger.info(s"Page Data Results:  \n${JsonUtil.toJson(data)}")


          cursorPos = s("after").toOpt
          if (cursorPos.isDefined) {
            logger.info(s"Page After Results:  \n${JsonUtil.toJson(cursorPos)}")
          }
        }
      }
    } while (cursorPos.isDefined)
  }


  val dcURL = "http://127.0.0.1:8443"
  val secret = "secret"
  val dbName = "LedgerExample"

  val dbSecret = createDatabase(dcURL, secret, dbName)

  val client = createDBClient(dcURL, dbSecret)

  createSchema(client)

  createCustomers(client)

  readCustomer(client, 1)

  readThreeCustomers(client, 1, 3, 8)

  readListOfCustomers(client, List(1, 3, 6, 7))

  readCustomersLessThan(client, 5)

  readCustomersBetween(client, 5, 11)

  readAllCustomers(client)

  /*
   * Just to keep things neat and tidy, close the client connections
   */
  client.close()
  logger.info(s"Disconnected from FaunaDB as server for DB ${dbName}!")

  // add this at the end of execution to make things shut down nicely
  System.exit(0)
}
