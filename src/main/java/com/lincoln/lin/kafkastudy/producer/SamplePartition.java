package com.lincoln.lin.kafkastudy.producer;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * description:
 *
 * @author linye
 * @date 2022年03月23日 10:53 上午
 */
public class SamplePartition implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        String keyStr = String.valueOf(key);
        String keyInt = keyStr.substring(5);
        System.out.println("keyStr : " + keyStr + "keyInt : " + keyInt);
        int i = Integer.parseInt(keyInt);
        return i%2;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
