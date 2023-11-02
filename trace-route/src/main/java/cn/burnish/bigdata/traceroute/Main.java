package cn.burnish.bigdata.traceroute;

import cn.burnish.bigdata.traceroute.core.TraceRouteProcessFunction;
import cn.burnish.bigdata.traceroute.datatypes.Segment;
import cn.burnish.bigdata.traceroute.datatypes.TraceRouteResult;
import cn.burnish.bigdata.traceroute.source.CapConfigBroadcastSource;
import cn.burnish.bigdata.traceroute.source.DataHubSourceFunction;
import cn.burnish.common.datatypes.CapConfigBroadcastData;
import cn.burnish.common.utils.ConfigLoader;
import cn.burnish.common.utils.JsonParser;
import com.aliyun.datahub.client.model.TupleRecordData;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Date;

public class Main extends ConfigLoader implements Serializable {
    private static final long serialVersionUID = 1L;
    public static Logger logger = LoggerFactory.getLogger(Main.class);

    public final ParameterTool parameterTool;

    private final SourceFunction<Segment> source;
    private final SinkFunction<TraceRouteResult> sink;


    // datahub config
    public static String configDatahubEndPoint;
    public static String configDatahubProjectName;
    public static String configDatahubTopicName;
    public static String configDatahubSubID;
    public static String configDatahubAccessID;
    public static String configDatahubAccessKey;
    public static Long configDatahubStartOffset;

    // mysql config
    public static String configMysqlHost;
    public static String configMysqlPort;
    public static String configMysqlDatabase;
    public static String configMysqlUser;
    public static String configMysqlPass;

    public Main(ParameterTool parameterTool, SourceFunction<Segment> source, SinkFunction<TraceRouteResult> sink) {
        this.parameterTool = parameterTool;
        this.source = source;
        this.sink = sink;
    }

    /**
     * Create and execute the trace route.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Segment> processDs = env
                .addSource(source)
                .name("dhSegmentInput");
//        DataStreamSource<String> dataStream = env.socketTextStream("127.0.0.1", 33069, "\n");
//        DataStream<TraceRouteInput> processDs = dataStream.map(
//                (MapFunction<String, TraceRouteInput>) s -> {
//                    String[] al = s.split(",");
//                    return new TraceRouteInput(
//                            al[0],
//                            Long.parseLong(al[1]),
//                            al[2],
//                            Double.parseDouble(al[3]),
//                            Double.parseDouble(al[4]),
//                            Double.parseDouble(al[5]),
//                            Double.parseDouble(al[6]),
//                            Double.parseDouble(al[7]),
//                            al[8],
//                            al[9]
//                    );
//                }
//        );

        // MYSQL 广播流
        DataStreamSource<CapConfigBroadcastData> statisticDataStream = env.
                addSource(new CapConfigBroadcastSource(configMysqlHost, configMysqlPort, configMysqlDatabase, configMysqlUser, configMysqlPass));

        MapStateDescriptor<Void, CapConfigBroadcastData> traceRouteBroadcastDescriptor =
                new MapStateDescriptor<>("traceRouteBroadcast", Void.class, CapConfigBroadcastData.class);

        BroadcastStream<CapConfigBroadcastData> traceRouteBroadcastDataStream = statisticDataStream.broadcast(traceRouteBroadcastDescriptor);


        DataStream<TraceRouteResult> outDs = processDs
                .keyBy(Segment::getVin)
                .connect(traceRouteBroadcastDataStream)
                .process(new TraceRouteProcessFunction());

        outDs.addSink(sink);

        outDs.print("行程输出：");

        return env.execute();
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool;
        if (args.length == 0) {
            parameterTool = ParameterTool.fromPropertiesFile("etc/trace-route.properties");
        }else {
            parameterTool = ParameterTool.fromArgs(args);
        }

        getConfig(parameterTool);

        Main runner = new Main(
                parameterTool,
                new DataHubSourceFunction<>(
                    configDatahubEndPoint, configDatahubAccessID, configDatahubAccessKey,
                    configDatahubProjectName, configDatahubTopicName, "1", configDatahubSubID,
                    null, recordEntry -> {
                    TupleRecordData recordData = (TupleRecordData) (recordEntry.getRecordData());
                    String event_code = (String) recordData.getField("event_code");
                    if (!event_code.equals("DRIVETIMER")) return null;        // 过滤掉不是 DRIVETIMER 的信号
                    Long obd_time = (Long) recordData.getField("pkg_ts");
                    String series = recordData.getField("veh_series").toString();
                    String vin = (String) recordData.getField("vin");
                    Date d = new Date();
                    if (obd_time == null || obd_time > d.getTime()) return null;  // 过滤掉时间为null以及obd_time为大于当前时间
                    String data = (String) recordData.getField("data");
                    Double mileage = JsonParser.getFieldValueFromJson(data, "V015", Double.class);
                    Double oil = JsonParser.getFieldValueFromJson(data, "T055", Double.class);
                    Double power = JsonParser.getFieldValueFromJson(data, "E010", Double.class);
                    Double urea = JsonParser.getFieldValueFromJson(data, "V112", Double.class);
                    Double speed = JsonParser.getFieldValueFromJson(data, "V019", Double.class);
                    String lng = JsonParser.getFieldValueFromJson(data, "GPS002", String.class);
                    String lat = JsonParser.getFieldValueFromJson(data, "GPS003", String.class);

                    return new Segment(vin, obd_time, series, mileage, oil, power, urea, speed, lng, lat);
                }),
                TraceRouteResult.sink(configMysqlHost, configMysqlPort, configMysqlDatabase, configMysqlUser, configMysqlPass)
        );

        System.out.println(runner.execute());
    }
}


