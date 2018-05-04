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
 * Read and set connection information so it does not have to be repeated in all of the examples
 */

import com.typesafe.config.ConfigFactory

object FaunaDBConfig {
  def getConfig = {

    val systemConfig = ConfigFactory.load()

    val rootKey = Option(systemConfig.getString("FAUNA_ROOT_KEY")) getOrElse {
      "secret"
    }
    val scheme = Option(systemConfig.getString("FAUNA_SCHEME")) getOrElse {
      "http"
    }
    val domain = Option(systemConfig.getString("FAUNA_DOMAIN")) getOrElse {
      "127.0.0.1"
    }
    val port = Option(systemConfig.getString("FAUNA_PORT")) getOrElse {
      "8443"
    }

    collection.Map(
      "root_token" -> rootKey,
      "root_url" -> s"${scheme}://${domain}:${port}"
    )

  }
}
