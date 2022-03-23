package com.lincoln.lin.kafkastudy.admin;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.protocol.types.Field.Str;

/**
 * description:
 *
 * @author linye
 * @date 2022年03月20日 5:02 下午
 */
public class AdminSample {

    public final static String TOPIC_NAME = "lincoln-topic";

//    public final static Integer partitions = 2;

    public static void main(String[] args) throws Exception {
        AdminClient adminClient = AdminSample.adminClient();
        System.out.println(adminClient);

        // 创建Topic实例
        createTopic();

//        //获取Topic列表
//        topicList();

//        //删除Topic列表
//        delTopics();
//
//        //获取Topic列表
//        topicList();

        // 描述topic
//        describeTopics();

        // 描述topic的配置
//        describeConfigTopics();

        // 修改Config信息
//        editDescribeConfigTopics();

        // 描述topic
//        describeTopics();

        // 增加partition数
        incrPartitions(2);

        // 描述topic
        describeTopics();
    }

    /**
     * Kafka中partition只能增加，不能修改和删除
     */
    public static void incrPartitions(Integer partitions) throws Exception{
        AdminClient adminClient = adminClient();
        Map<String, NewPartitions> partitionsMap = new HashMap<>();
        NewPartitions newPartitions = NewPartitions.increaseTo(partitions);
        partitionsMap.put(TOPIC_NAME, newPartitions);


        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(partitionsMap);
        createPartitionsResult.all().get();
    }

    /**
     * 修改Config信息
     * @throws Exception
     */
    public static void editDescribeConfigTopics() throws  Exception {
        AdminClient adminClient = adminClient();
//        Map<ConfigResource, Config> configMaps = new HashMap<>();
//
//        // 组织两个参数
//        ConfigResource configResource = new ConfigResource(Type.TOPIC, TOPIC_NAME);
//        Config config = new Config(Lists.newArrayList(new ConfigEntry("preallocate", "true")));
//        configMaps.put(configResource, config);

        // 2.3 以上修改的API
        Map<ConfigResource, Collection<AlterConfigOp>> configMaps = new HashMap<>();
        // 组织两个参数
        ConfigResource configResource = new ConfigResource(Type.TOPIC, TOPIC_NAME);
        AlterConfigOp alterConfigOp = new AlterConfigOp(new ConfigEntry("preallocate", "false"), OpType.SET);
        configMaps.put(configResource, Lists.newArrayList(alterConfigOp));
        AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(configMaps);
        alterConfigsResult.all().get();
    }


    /**
     * describeTopics & describeConfigTopics 主要用来Kafka监控来用
     */

    /**
     * 描述topic
     * name=lincoln-topic,
     * internal=false, (是否内部)
     * partitions=(partition=0,
     *  leader=172.16.117.4:9092 (id: 0 rack: null),
     *  replicas=172.16.117.4:9092 (id: 0 rack: null),
     *  isr=172.16.117.4:9092 (id: 0 rack: null)),
     *  authorizedOperations=null)
     */
    public static void describeTopics() throws Exception {
        AdminClient adminClient = adminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Lists.newArrayList(TOPIC_NAME));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();
        entries.stream().forEach(System.out::println);
    }

    /**
     * 描述topic的配置
     * ConfigResource(type=TOPIC, name='lincoln-topic'),
     * Config(
     *  entries=[
     *      ConfigEntry(
     *          name=compression.type,
     *          value=producer,
     *          source=DEFAULT_CONFIG,
     *          isSensitive=false,
     *          isReadOnly=false,
     *          synonyms=[], t
     *          ype=STRING,
     *          documentation=null),
 *          ConfigEntry(
     *          name=leader.replication.throttled.replicas,
     *          value=,
     *          source=DEFAULT_CONFIG,
     *          isSensitive=false,
     *          isReadOnly=false,
     *          synonyms=[],
     *          type=LIST,
     *          documentation=null), ConfigEntry(name=remote.storage.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=BOOLEAN, documentation=null), ConfigEntry(name=message.downconversion.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=BOOLEAN, documentation=null), ConfigEntry(name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=INT, documentation=null), ConfigEntry(name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null), ConfigEntry(name=local.retention.ms, value=-2, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null), ConfigEntry(name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LIST, documentation=null), ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null), ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LIST, documentation=null), ConfigEntry(name=segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=INT, documentation=null), ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null), ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null), ConfigEntry(name=message.format.version, value=3.0-IV1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=STRING, documentation=null), ConfigEntry(name=max.compaction.lag.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null), ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null), ConfigEntry(name=max.message.bytes, value=1048588, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=INT, documentation=null), ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null), ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=STRING, documentation=null), ConfigEntry(name=local.retention.bytes, value=-2, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null), ConfigEntry(name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=BOOLEAN, documentation=null), ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=DOUBLE, documentation=null), ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=INT, documentation=null), ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=BOOLEAN, documentation=null), ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null), ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null), ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null), ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null), ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=INT, documentation=null)])
     */
    public static void describeConfigTopics() throws  Exception {
        AdminClient adminClient = adminClient();
        //TODO 这里是一个预留，集群会有
//        ConfigResource configResource = new ConfigResource(Type.BROKER, TOPIC_NAME);
        ConfigResource configResource = new ConfigResource(Type.TOPIC, TOPIC_NAME);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Lists.newArrayList(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
        configResourceConfigMap.entrySet().stream().forEach( entry -> {
            System.out.println("configResource : " + entry.getKey() + ", Config : " + entry.getValue());
        } );
    }

    /**
     * 删除topic
     */
    public static void delTopics() throws Exception {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Lists.newArrayList(TOPIC_NAME));
        System.out.println(deleteTopicsResult.all().get());
    }

    /**
     * 获取topic列表
     * @throws Exception
     */
    public static void topicList() throws Exception{
        AdminClient adminClient = adminClient();
        // 是否查看internal选项
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
//        ListTopicsResult listTopicsResult = adminClient.listTopics();
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        Set<String> names =  listTopicsResult.names().get();
        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        names.forEach(System.out::println);

        topicListings.forEach(System.out::println);
    }

    /**
     * 创建topic 实例
     */
    public static void createTopic() {
        AdminClient adminClient = adminClient();
        // 副本因子
        Short rs = 1;
        NewTopic newTopic = new NewTopic(TOPIC_NAME,1,rs);
        CreateTopicsResult topicsResult = adminClient.createTopics(Collections.singletonList(newTopic));
        System.out.println("topicsResult:" + topicsResult);
    }

    /**
     * 设置AdminClient
     * @return
     */
    public static AdminClient adminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.117.4:9092");

        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

}
