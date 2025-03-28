#!/bin/sh

cat > /shell/taildir_position.json <<EOF
[]
EOF
chmod 777 /shell/taildir_position.json &&
touch /shell/flume_log_kafka_hdfs.conf &&
cat > /shell/flume_log_kafka_hdfs.conf <<EOF
a1.sources = r1
a1.channels = c1
a1.sinks = k1

a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1

a1.sources.r1.type = TAILDIR
a1.sources.r1.positionFile = /shell/taildir_position.json
a1.sources.r1.filegroups = f1
a1.sources.r1.filegroups.f1 = /shell/app.2025-03-28.log
a1.sources.r1.headers.f1.headerKey1 = log
a1.sources.r1.fileHeader = true
a1.sources.ri.maxBatchCount = 5000


a1.channels.c1.type = org.apache.flume.channel.kafka.KafkaChannel
a1.channels.c1.kafka.bootstrap.servers = cdh03:9092
a1.channels.c1.kafka.topic = kafka_channel
a1.channels.c1.kafka.consumer.group.id = flume-consumer
a1.channels.c1.parseAsFlumeEvent = false


a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /flume/events/%y-%m-%d
a1.sinks.k1.hdfs.filePrefix = log-
a1.sinks.k1.hdfs.fileType = DataStream
a1.sinks.k1.hdfs.useLocalTimeStamp = true
a1.sinks.k1.hdfs.round = true
a1.sinks.k1.hdfs.roundValue = 10
a1.sinks.k1.hdfs.roundUnit = minute
a1.sinks.k1.hdfs.rollInterval = 30
a1.sinks.k1.hdfs.rollSize = 204800
a1.sinks.k1.hdfs.rollCount = 1000
a1.sinks.k1.hdfs.batchSize = 1000
EOF
#启动flume_ng
sudo -u hdfs flume-ng agent --conf /opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/etc/flume-ng/conf.empty/ \
--conf-file /shell/flume_log_kafka_hdfs.conf \
--name a1 \
-Dflume.root.logger=INFO,console