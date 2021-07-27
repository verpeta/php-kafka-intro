<?php

$conf = new RdKafka\Conf();

$conf->set('group.id', 'group_1');
$conf->set('log_level', (string)LOG_DEBUG);
$conf->set('metadata.broker.list', 'kafka:9092');
//$conf->set('debug', 'all');

$rk = new RdKafka\Consumer($conf);

$topicConf = new RdKafka\TopicConf();
$topicConf->set('auto.commit.interval.ms', 9999999);

// Set where to start consuming messages when there is no initial offset in
// offset store or the desired offset is out of range.
// 'smallest': start from the beginning
$topicConf->set('auto.offset.reset', 'smallest');
$topic = $rk->newTopic("helloworld", $topicConf);

$partition = 0;

// The first argument is the partition to consume from.
// The second argument is the offset at which to start consumption. Valid values
// are: RD_KAFKA_OFFSET_BEGINNING, RD_KAFKA_OFFSET_END, RD_KAFKA_OFFSET_STORED.
$topic->consumeStart($partition, RD_KAFKA_OFFSET_STORED);

while (true) {
    // The first argument is the partition (again).
    // The second argument is the timeout.
    $msg = $topic->consume($partition, 1000);

    if (null === $msg || $msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        // Constant check required by librdkafka 0.11.6. Newer librdkafka versions will return NULL instead.
        print_r('sleep...');
        sleep(2);
        continue;
    } elseif ($msg->err) {
        echo $msg->errstr(), "\n";
        break;
    } else {
        echo 'partition -' . $msg->partition, "\n";
        echo $msg->payload, "\n";
        // Manually save offset(aka ack in RabbitMQ)
        $topic->offsetStore($partition, $msg->offset);
    }
}
