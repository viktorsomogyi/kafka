/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.api

import java.util

import kafka.admin.AclCommand
import kafka.server.KafkaConfig
import kafka.utils.{JaasTestUtils, TestUtils, ZkUtils}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, CreateDelegationTokenOptions}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.DelegationToken
import org.junit.Before

import scala.collection.JavaConverters._

class DelegationTokenEndToEndAuthorizationWithOwnerTest extends EndToEndAuthorizationTest {

  val kafkaClientSaslMechanism = "SCRAM-SHA-256"
  val kafkaServerSaslMechanisms = ScramMechanism.mechanismNames.asScala.toList
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  //token owner principal name
  override val clientPrincipal = "owner1"

  private val tokenRequestPrincipal = JaasTestUtils.KafkaScramUser
  private val tokenRequestPrincipalPassword = JaasTestUtils.KafkaScramPassword

  override val kafkaPrincipal = JaasTestUtils.KafkaScramAdmin
  private val kafkaPassword = JaasTestUtils.KafkaScramAdminPassword

  this.serverConfig.setProperty(KafkaConfig.DelegationTokenMasterKeyProp, "testKey")

  def createTokenArgs: Array[String] = Array("--authorizer-properties",
                                          s"zookeeper.connect=$zkConnect",
                                          s"--add",
                                          s"--cluster",
                                          s"--operation=CreateTokens",
                                          s"--allow-principal=$kafkaPrincipalType:$tokenRequestPrincipal")

  override def configureSecurityBeforeServersStart() {
    super.configureSecurityBeforeServersStart()
    zkClient.makeSurePersistentPathExists(ZkUtils.ConfigChangesPath)
    // Create broker admin credentials before starting brokers
    createScramCredentials(zkConnect, kafkaPrincipal, kafkaPassword)

    //Create Acls for CreateTokens Operation for tokenRequestPrincipal
   AclCommand.main(createTokenArgs)

  }

  override def configureSecurityAfterServersStart() {
    super.configureSecurityAfterServersStart()

    // create scram credential for user "scram-user"
    createScramCredentials(zkConnect, tokenRequestPrincipal, tokenRequestPrincipalPassword)

    //create a token with "scram-user" credentials for user "owner1"
    val token = createDelegationToken()

    // pass token to client jaas config
    val clientLoginContext = JaasTestUtils.tokenClientLoginModule(token.tokenInfo().tokenId(), token.hmacAsBase64String())
    producerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
    consumerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
  }

  @Before
  override def setUp() {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Option(kafkaClientSaslMechanism), Both))
    super.setUp()
  }

  private def createDelegationToken(): DelegationToken = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    securityProps.asScala.foreach { case (key, value) => config.put(key.asInstanceOf[String], value) }
    val clientLoginContext = jaasClientLoginModule(kafkaClientSaslMechanism)
    config.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)

    val adminClient = AdminClient.create(config)
    val ownerPrincipal = new KafkaPrincipal(kafkaPrincipalType, clientPrincipal)
    val createTokenOptions = new CreateDelegationTokenOptions().owner(ownerPrincipal)
    val token = adminClient.createDelegationToken(createTokenOptions).delegationToken().get()
    //wait for token to reach all the brokers
    TestUtils.waitUntilTrue(() => servers.forall(server => !server.tokenCache.tokens().isEmpty),
      "Timed out waiting for token to propagate to all servers")
    adminClient.close()

    token
  }
}
