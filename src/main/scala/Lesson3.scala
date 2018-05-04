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
import scala.util.{Success, Try}

/*
 * These are the required imports for Fauna.
 *
 * For these examples we are using the 2.1.0 version of the JVM driver. Also notice that we aliasing
 * the query and values part of the API to make it more obvious we we are using Fauna functionality.
 *
 */
import faunadb.{FaunaClient, query => q, values => v}

object Lesson3 extends App with Logging {
  import ExecutionContext.Implicits._

  /*
   * Get the configuration values for FaunaDB contained in the application.conf file.
   */
  val config = FaunaDBConfig.getConfig

  logger.info(s"FaunaDB root URL: ${config("root_url")}")
  logger.info(s"FaunaDB security key: ${config("root_token")}")


  /*
   * Create an admin client. This is the client we will use to create the database.
   *
   * If you are using the the FaunaDB-Cloud you will need to replace the value of the
   * 'secret' in the command below with your "secret".
   */
  val adminClient = FaunaClient(endpoint = config("root_url"), secret = config("root_token"))
  logger.info("Connected to FaunaDB as Admin!")

  /*
   * The code below creates the Database that will be used for this example. Please note that
   * the existence of the database is evaluated, deleted if it exists and recreated with a single
   * call to the Fauna DB.
   */
  val dbName = "LedgerExample"

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
  val serverKey = key(v.Field("secret").to[String]).get
  logger.info(s"DB ${dbName} secret: ${serverKey}")

  /*
   * Create the DB specific DB client using the DB specific key just created.
   */
  val client = FaunaClient(endpoint = config("root_url"), secret = serverKey)

  /*
   * Create an class to hold customers
   */
  queryResponse = client.query(
    q.CreateClass(q.Obj("name" -> "customers"))
  )
  Await.result(queryResponse, Duration.Inf)
  logger.info(s"Created customer class :: \n${JsonUtil.toJson(queryResponse)}")

  /*
   * Create the Indexes within the database. We will use these to access record in later lessons
   */
  queryResponse = client.query(
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
  Await.result(queryResponse, Duration.Inf)
  logger.info(s"Created customer_by_id index :: \n${JsonUtil.toJson(queryResponse)}")

  /*
   * Create 20 customer records with ids from 1 to 20
   */
  Await.result(
    client.query(
      q.Map((1 to 20).toList,
        q.Lambda { id =>
          q.Create(
            q.Class("customers"),
            q.Obj("data" -> q.Obj("id" -> id, "balance" -> q.Multiply(id, 10)))
          )
        }
      )
    ), Duration.Inf)

  /*
   * Read a single record and return the data it holds
   * We saw this from the previous Lesson code
   */
  val custID = 1
  queryResponse = client.query(
    q.Select("data", q.Get(q.Match(q.Index("customer_by_id"), custID)))
  )
  Await.result(queryResponse, Duration.Inf)
  logger.info(s"Read \'customer\' ${custID}: \n${JsonUtil.toJson(queryResponse)}")

  /*
   * Here is a more general use case where we retrieve multiple class references
   * by id and return the actual data underlying them.
   */
  queryResponse = client.query(
    q.Map(
      q.Paginate(
        q.Union(
          q.Match(q.Index("customer_by_id"), 1),
          q.Match(q.Index("customer_by_id"), 3),
          q.Match(q.Index("customer_by_id"), 8)
        )
      ),
      q.Lambda { x => q.Select("data", q.Get(x)) }
    )
  )
  Await.result(queryResponse, Duration.Inf)
  logger.info(s"Union specific \'customer\' 1, 3, 8: \n${JsonUtil.toJson(queryResponse)}")

  /*
   * Finally a much more general use case where we can supply any number of id values
   * and return the data for each.
   */
  val range = List(1, 3, 6, 7)
  queryResponse = client.query(
    q.Map(
      q.Paginate(
        q.Union(
          q.Map(range,
            q.Lambda { y => q.Match(q.Index("customer_by_id"), y) }
          )
        )
      ),
      q.Lambda { x => q.Select("data", q.Get(x)) }
    )
  )
  Await.result(queryResponse, Duration.Inf)
  logger.info(s"Union variable \'customer\' ${range}: \n${JsonUtil.toJson(queryResponse)}")

  /*
   * In this example we use the values based filter 'customer_id_filter'.
   * using this filter we can query by range. This is an example of returning
   * all the values less than(<) or before 5. The keyword 'after' can replace
   * 'before' to yield the expected results.
   */
  queryResponse = client.query(
    q.Map(
      q.Paginate(q.Match(q.Index("customer_id_filter")), cursor = q.Before(5)),
      q.Lambda { x => q.Select("data", q.Get(q.Select(1, x))) }
    )
  )
  Await.result(queryResponse, Duration.Inf)
  logger.info(s"Query for id\'s < 5 : \n${JsonUtil.toJson(queryResponse)}")

  /*
   * Extending the previous example to show getting a range between two values.
   */
  queryResponse = client.query(
    q.Map(
      q.Filter(q.Paginate(q.Match(q.Index("customer_id_filter")), cursor = q.Before(11)),
        q.Lambda { y => q.LTE(5, q.Select(0, y)) } ),
      q.Lambda { x => q.Select("data", q.Get(q.Select(1, x))) }
    )
  )
  Await.result(queryResponse, Duration.Inf)
  logger.info(s"Query for id\'s >= 5  and < 11 : \n${JsonUtil.toJson(queryResponse)}")

  /*
   * Read all the records that we created.
   * Use a small'ish page size so that we can demonstrate a paging example.
   *
   * NOTE: after is inclusive of the value.
   */
  implicit val customerCodec = v.Codec.caseClass[Customer]
  case class Customer(id: Int, balance: Int)

  val pageSize = 8
  var cursorPos: Long = 1L
  var morePages = false
  var balanceSum = 0
  do {
    val result = Try {
        val stmt = client.query(
          q.Map(
            q.Paginate(q.Match(q.Index("customer_id_filter")), cursor = q.After(cursorPos), size = pageSize),
            q.Lambda { x => q.Select("data", q.Get(q.Select(1, x))) }
          )
        )
        Await.result(stmt, Duration.Inf)
      } match {
        case Success(s) => {
          try {
            val data = s("data").to[List[Customer]].get
            for ( c <- data){
              balanceSum += c.balance
              println(s"Customer(id -> ${c.id} :: balance -> ${c.balance})")
            }

            val after = s("after").to[v.ArrayV].get
            cursorPos = after.elems.head.to[Long].get
            morePages = true
          } catch {
            case e: v.ValueReadException => { morePages = false }
          }
        }
    }
  } while (morePages)
  println(s"Total of all balances: ${balanceSum}")

  /*
   * Just to keep things neat and tidy, close the client connections
   */
  client.close()
  logger.info(s"Disconnected from FaunaDB as server for DB ${dbName}!")
  adminClient.close()
  logger.info("Disconnected from FaunaDB as Admin!")

  // add this at the end of execution to make things shut down nicely
  System.exit(0)
}
