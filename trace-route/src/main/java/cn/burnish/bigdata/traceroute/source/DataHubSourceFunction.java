package cn.burnish.bigdata.traceroute.source;

import cn.burnish.bigdata.traceroute.Main;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.*;
import com.aliyun.datahub.client.model.*;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataHubSourceFunction<T> implements SourceFunction<T> {

    public static Logger logger = LoggerFactory.getLogger(Main.class);

    private String accessId;
    private String accessKey;
    private String endpoint;
    private String projectName;
    private String topicName;

    private String subId;
    private Long startOffsetMs = null;

    private int maxRetry = 5;
    private String shardId = "1";

    private int limit = 1000;

    private DatahubConfig conf;
    private DatahubClient client;
    private volatile boolean isRunning = true;
    private RecordMapper<T> recordMapper;
    public DataHubSourceFunction(
            String endpoint,
            String accessId,
            String accessKey,
            String projectName,
            String topicName,
            String shardId,
            String subId,
            Long startOffsetMs,
            RecordMapper<T> recordMapper
    ) {
        this.recordMapper = recordMapper;
        this.endpoint = endpoint;
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.projectName = projectName;
        this.topicName = topicName;
        this.shardId = shardId;
        this.subId = subId;
        if (startOffsetMs != null && startOffsetMs > 0) this.startOffsetMs = startOffsetMs;

        conf = new DatahubConfig(this.endpoint, new AliyunAccount(this.accessId, this.accessKey));
        client = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(conf)
//                .setHttpConfig()
                .build();
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
            List<String> shardIds = Arrays.asList(new String[]{shardId,});
            GetTopicResult topicResult = client.getTopic(projectName, topicName);
            // 首先初始化offset上下文
            GetSubscriptionOffsetResult offsetResult = client.getSubscriptionOffset(projectName, topicName, subId, shardIds);
            OpenSubscriptionSessionResult openSubscriptionSessionResult = client.openSubscriptionSession(projectName, topicName, subId, shardIds);
            SubscriptionOffset subscriptionOffset = openSubscriptionSessionResult.getOffsets().get(shardId);
            // 1、获取当前点位的cursor，如果当前点位已过期则获取生命周期内第一条record的cursor，未消费同样获取生命周期内第一条record的cursor
            String cursor = "";
            //sequence < 0说明未消费
            if (subscriptionOffset.getSequence() < 0) {
                // 获取生命周期内第一条record的cursor
                cursor = client.getCursor(projectName, topicName, shardId, CursorType.OLDEST).getCursor();
            } else if (startOffsetMs != null) {
                try {
                    cursor = client.getCursor(projectName, topicName, shardId, CursorType.SYSTEM_TIME, startOffsetMs).getCursor();
                } catch (SeekOutOfRangeException e) {
                    cursor = client.getCursor(projectName, topicName, shardId, CursorType.OLDEST).getCursor();
                }
            } else {
                // 获取下一条记录的Cursor
                long nextSequence = subscriptionOffset.getSequence() + 1;
                try {
                    //按照SEQUENCE getCursor可能报SeekOutOfRange错误，表示当前cursor的数据已过期
                    cursor = client.getCursor(projectName, topicName, shardId, CursorType.SEQUENCE, nextSequence).getCursor();
                } catch (SeekOutOfRangeException e) {
                    // 获取生命周期内第一条record的cursor
                    cursor = client.getCursor(projectName, topicName, shardId, CursorType.OLDEST).getCursor();
                }
            }

            System.out.println("Start consume records, begin cursor :" + cursor);
        // 2、读取并保存点位，这里以读取Tuple数据为例，并且每1000条记录保存一次点位
        long recordCount = 0L;
        // 每次读取1000条record
        int retryNum = 0;
        int commitNum = 1000;
        while (retryNum < maxRetry && isRunning) {
            try {
                GetRecordsResult getRecordsResult = client.getRecords(projectName, topicName, shardId, cursor, limit);
                if (getRecordsResult.getRecordCount() <= 0) {
                    // 无数据，sleep后读取
                    System.out.println("no data, sleep 1 second");
                    Thread.sleep(1000);
                    continue;
                }
                for (RecordEntry recordEntry : getRecordsResult.getRecords()) {
                    //消费数据
                    T value = recordMapper.mapRecord(recordEntry);
                    if (value != null) ctx.collect(value);
                    // 处理数据完成后，设置点位
                    recordCount++;
                    subscriptionOffset.setSequence(recordEntry.getSequence());
                    subscriptionOffset.setTimestamp(recordEntry.getSystemTime());
                    // commit offset every 1000 records
                    if (recordCount % commitNum == 0) {
                        //提交点位点位
                        Map<String, SubscriptionOffset> offsetMap = new HashMap<>();
                        offsetMap.put(shardId, subscriptionOffset);
                        client.commitSubscriptionOffset(projectName, topicName, subId, offsetMap);
                        System.out.println("commit offset successful");
                    }
                }
                cursor = getRecordsResult.getNextCursor();
            } catch (SubscriptionOfflineException | SubscriptionSessionInvalidException e) {
                // 退出. Offline: 订阅下线; SessionChange: 表示订阅被其他客户端同时消费
                logger.error("Critical: Offline or SessionInvalid", e);
                throw e;
            } catch (SubscriptionOffsetResetException e) {
                // 点位被重置，需要重新获取SubscriptionOffset版本信息
                SubscriptionOffset offset = client.getSubscriptionOffset(projectName, topicName, subId, shardIds).getOffsets().get(shardId);
                subscriptionOffset.setVersionId(offset.getVersionId());
                // 点位被重置之后，需要重新获取点位，获取点位的方法应该与重置点位时一致，
                // 如果重置点位时，同时设置了sequence和timestamp，那么既可以用SEQUENCE获取，也可以用SYSTEM_TIME获取
                // 如果重置点位时，只设置了sequence，那么只能用sequence获取，
                // 如果重置点位时，只设置了timestamp，那么只能用SYSTEM_TIME获取点位
                // 一般情况下，优先使用SEQUENCE，其次是SYSTEM_TIME,如果都失败，则采用OLDEST获取
                cursor = null;
                try {
                    long nextSequence = offset.getSequence() + 1;
                    cursor = client.getCursor(projectName, topicName, shardId, CursorType.SEQUENCE, nextSequence).getCursor();
                    System.out.println("get cursor successful");
                } catch (DatahubClientException exception) {
                    System.out.println("get cursor by SEQUENCE failed, try to get cursor by SYSTEM_TIME");
                }
                if (cursor == null) {
                    try {
                        cursor = client.getCursor(projectName, topicName, shardId, CursorType.SYSTEM_TIME, offset.getTimestamp()).getCursor();
                        System.out.println("get cursor successful");
                    } catch (DatahubClientException exception) {
                        System.out.println("get cursor by SYSTEM_TIME failed, try to get cursor by OLDEST");
                    }
                }
                if (cursor == null) {
                    try {
                        cursor = client.getCursor(projectName, topicName, shardId, CursorType.OLDEST).getCursor();
                        System.out.println("get cursor successful");
                    } catch (DatahubClientException exception) {
                        System.out.println("get cursor by OLDEST failed");
                        System.out.println("get cursor failed!!");
                        throw e;
                    }
                }
            } catch (LimitExceededException e) {
                // limit exceed, retry
                logger.error("Retry: Limit Exceed", e);
                retryNum++;
            } catch (DatahubClientException e) {
                // other error, retry
                logger.error("Retry: Other", e);
                retryNum++;
            } catch (Exception e) {
                logger.error("Critical: Other", e);
                System.exit(-1);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public interface RecordMapper<T> {
        T mapRecord(RecordEntry resultEntry) throws Exception;
    }
}
