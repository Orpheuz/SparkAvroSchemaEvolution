package kafka

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import scala.jdk.CollectionConverters._

class DemoKafkaAdmin(
  bootstrapServers: String,
  topicName: String) {

  val kafkaAdmin: AdminClient = AdminClient.create(
    Map[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers).asJava
  )

  def createTopic(): Unit = {

    kafkaAdmin.createTopics(List(new NewTopic(topicName, 3, 2)).asJava).all().get()
  }

  def deleteIfExists(): Unit = {

    if(kafkaAdmin.listTopics().names().get().contains(topicName)) {
      kafkaAdmin.deleteTopics(List(topicName).asJava).all().get()
    }
  }
}
