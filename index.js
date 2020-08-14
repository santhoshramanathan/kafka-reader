require("dotenv").config();
const fetch = require("node-fetch");
const kafka = require("kafka-node");

const middleware_url = process.env.MIDDLEWARE_URL;

const Consumer = kafka.Consumer,
  // The client specifies the ip of the Kafka producer and uses
  // the zookeeper port 2181
  client = new kafka.KafkaClient("broker-zookeeper-client.debezium:2181"),
  // The consumer object specifies the client and topic(s) it subscribes to
  consumer = new Consumer(
    client,
    [{ topic: "dbserver1.inventory.customers", partition: 0 }],
    {
      autoCommit: false,
    }
  );

consumer.on("message", async function (message) {
  // grab the main content from the Kafka message
  var data = JSON.parse(message.value);
  console.log(data);
  // const result = await fetch(`${middleware_url}/book`, {});
  // console.log("DB update result...", result);
});
