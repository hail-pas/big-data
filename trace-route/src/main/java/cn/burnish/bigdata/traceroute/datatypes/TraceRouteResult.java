package cn.burnish.bigdata.traceroute.datatypes;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@AllArgsConstructor
public class TraceRouteResult {
    public String vin; // vin
    public Long startObdTime;  //  起始点时间
    public String startLng;  // 起始点经度
    public String startLat;  // 起始点纬度
    public Long endObdTime;  //  结束点时间
    public String endLng;  // 结束点经度
    public String endLat;  // 结束点纬度
    public Long stopObdTime;  // 最后停车时间

    public Integer companyId;  // 企业ID
    public Integer teamId;  // 企业ID
    public Integer truckId;  // 车辆ID

    public Double mileage;  // 行驶里程
    public Double oil; // 油耗
    public Double power; // 电耗
    public Double powerKwh; // 电耗(kwh)
    public Double urea; // 尿耗
    public Double maxSpeed;  // 最大速度
    public Long delayTime; // 怠速时长


    public Long obdTime; // 更新数据的时候的obdTime

    public static Logger logger = LoggerFactory.getLogger(TraceRouteResult.class);


    public static SinkFunction<TraceRouteResult> sink(
            String configMysqlHost,
            String configMysqlPort,
            String configMysqlDatabase,
            String configMysqlUser,
            String configMysqlPass) {
        return JdbcSink.sink(
                // 没有插入，存在更新
                "insert into monitor_traceroute (vin,start_obd_timestamp,start_lng,start_lat,end_obd_timestamp," +
                        "mileage,max_speed,end_lng,end_lat,stop_obd_timestamp,deleted,create_time,update_time,start_address,end_address," +
                        "oil,urea,power,power_kwh,delay_time,company_id,team_id,truck_id,run_time,stop_time) " +
                        "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE stop_obd_timestamp=(?), stop_time=(?)",
                (ps, value) -> {
                    logger.info(String.format("写入数据: %s", value));
                    ps.setString(1, value.vin);
                    ps.setLong(2, value.startObdTime);
                    ps.setString(3, value.startLng);
                    ps.setString(4, value.startLat);
                    ps.setLong(5, value.endObdTime);
                    ps.setDouble(6, value.mileage);
                    ps.setDouble(7, value.maxSpeed);
                    ps.setString(8, value.endLng);
                    ps.setString(9, value.endLat);
                    ps.setLong(10, value.stopObdTime);
                    ps.setBoolean(11, false);
                    java.util.Date date = new java.util.Date();
                    java.sql.Timestamp stamp = new java.sql.Timestamp(date.getTime());
                    ps.setTimestamp(12, stamp);
                    ps.setTimestamp(13, stamp);
                    ps.setString(14, "");
                    ps.setString(15, "");
                    ps.setDouble(16, value.oil);
                    ps.setDouble(17, value.urea);
                    ps.setDouble(18, value.power);
                    ps.setDouble(19, value.powerKwh);
                    ps.setLong(20, value.delayTime);
                    ps.setLong(21, value.companyId);
                    // 处理teamId 为null的值
                    if (value.teamId == 0) {
                        ps.setNull(22, 12);
                    }else{
                        ps.setLong(22, value.teamId);
                    }
                    ps.setLong(23, value.truckId);
                    ps.setLong(24, value.endObdTime - value.startObdTime);
                    ps.setLong(25, value.stopObdTime - value.endObdTime);
                    ps.setLong(26, value.stopObdTime);
                    ps.setLong(27, value.stopObdTime - value.endObdTime);
                    logger.info(String.format("写入SQL: %s", ps));
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(String.format("jdbc:mysql://%s:%s/%s?useSSL=false&enabledTLSProtocols=TLSv1,TLSv1.1,TLSv1.2,TLSv1.3&characterEncoding=utf8", configMysqlHost, configMysqlPort, configMysqlDatabase))  // ip:host/database
                        .withDriverName("com.mysql.cj.jdbc.Driver")  // Driver
                        .withUsername(configMysqlUser)
                        .withPassword(configMysqlPass)
                        .build());
    }
}


