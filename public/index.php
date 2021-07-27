<?php

error_reporting(E_ALL);

use RdKafka\Producer;
use RdKafka\TopicConf;

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'kafka:9092');

$producer = new Producer($conf);

$tc = new TopicConf();

$topic = $producer->newTopic("helloworld");

if (!$producer->getMetadata(false, $topic, 2000)) {
    echo "Failed to get metadata, is broker down?\n";
    exit;
}

$i = 0;
while ($i < 100) {
    $i++;
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, 'lol' . $i);
}

$producer->flush(200);

echo "Message published\n";
