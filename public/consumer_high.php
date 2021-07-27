<?php
$conf = new RdKafka\Conf();

// Set a rebalance callback to log partition assignments (optional)
$conf->setRebalanceCb(function (RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
    switch ($err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            echo "Assign: " . PHP_EOL;
            var_dump($partitions);
            $kafka->assign($partitions);
            break;
        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            echo "Revoke: " . PHP_EOL;
           // var_dump($partitions);
            $kafka->assign();
            break;
        default:
            throw new \Exception($err);
    }
});

// Configure the group.id. All consumer with the same group.id will consume
// different partitions.
$conf->set('group.id', 'group_1');

// Initial list of Kafka brokers
$conf->set('metadata.broker.list', 'kafka:9092');
// Required for manual acknowledge
$conf->set('enable.auto.commit', 'false');

$topicConf = new RdKafka\TopicConf();

// Set where to start consuming messages when there is no initial offset in
// offset store or the desired offset is out of range.
// 'smallest': start from the beginning
$topicConf->set('auto.offset.reset', 'smallest');

$conf->setDefaultTopicConf($topicConf);

$consumer = new RdKafka\KafkaConsumer($conf);

// Subscribe to topic 'helloworld'
$consumer->subscribe(['helloworld']);

echo "Waiting for partition assignment... (make take some time when\n";
echo "quickly re-joining the group after leaving it.)\n";

while (true) {
    $message = $consumer->consume(3e3);
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            echo $message->payload . PHP_EOL;
            echo 'pt -- ' . $message->partition . PHP_EOL;
            // Save offset
            $consumer->commit($message);
            break;
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            sleep(2);
            echo 'sleep'. PHP_EOL;
            continue;
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            echo $message->errstr() . PHP_EOL;
            break;
        default:
            throw new \Exception($message->errstr(), $message->err);
    }
}