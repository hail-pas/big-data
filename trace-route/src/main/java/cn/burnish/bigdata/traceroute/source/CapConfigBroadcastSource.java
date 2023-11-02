package cn.burnish.bigdata.traceroute.source;

import cn.burnish.common.datatypes.CapConfigBroadcastData;
import cn.burnish.common.datatypes.SAPPowerData;
import cn.burnish.common.datatypes.SeriesStandData;
import cn.burnish.common.datatypes.TruckData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.HashMap;

public class CapConfigBroadcastSource extends RichSourceFunction<CapConfigBroadcastData> {
    private Connection connection;
    private PreparedStatement seriesStandPreparedStatement;  // 车系的邮箱电耗配置
    private PreparedStatement sapPowerPreparedStatement;  // 更加SAP获取车型电量配置
    private PreparedStatement truckPreparedStatement;  // 车辆信息配置

    private final String configMysqlHost;
    private final String configMysqlDatabase;
    private final String configMysqlPort;
    private final String configMysqlUser;
    private final String configMysqlPass;


    public CapConfigBroadcastSource(String configMysqlHost, String configMysqlPort, String configMysqlDatabase, String configMysqlUser, String configMysqlPass) {
        this.configMysqlHost = configMysqlHost;
        this.configMysqlPort = configMysqlPort;
        this.configMysqlDatabase = configMysqlDatabase;
        this.configMysqlUser = configMysqlUser;
        this.configMysqlPass = configMysqlPass;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(String.format("jdbc:mysql://%s:%s/%s?useSSL=false&enabledTLSProtocols=TLSv1,TLSv1.1,TLSv1.2,TLSv1.3&characterEncoding=utf8", configMysqlHost, configMysqlPort, configMysqlDatabase), configMysqlUser, configMysqlPass);
        String sql = "select * from  other_vehicleseriesstandard";
        seriesStandPreparedStatement = connection.prepareStatement(sql);
        sql = "select * from  other_sapcodeflagtobatterystandard";
        sapPowerPreparedStatement = connection.prepareStatement(sql);
        sql = "select id, vin, company_id, team_id from truck_truck where truck_truck.deleted = False";
        truckPreparedStatement = connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<CapConfigBroadcastData> sourceContext) throws Exception {
        while (true) {
            HashMap<String, SeriesStandData> seriesStandMap = new HashMap<>();
            try {
                ResultSet resultSet = seriesStandPreparedStatement.executeQuery();
                while (resultSet.next()) {
                    String series = resultSet.getString("vehicle_series");
                    Double maxOil = resultSet.getDouble("fuel_tank_volume");
                    Double maxUrea = resultSet.getDouble("urea_tank_volume");
                    seriesStandMap.put(series, new SeriesStandData(series, maxOil, maxUrea));
                }
            } finally {
            }
            HashMap<Character, SAPPowerData> sapPowerMap = new HashMap<>();
            try {
                ResultSet resultSet = sapPowerPreparedStatement.executeQuery();
                while (resultSet.next()) {
                    Character sapCode = resultSet.getString("sap_code_flag").charAt(0);
                    Double kwhCap = resultSet.getDouble("battery_pkg_capacity");
                    Double ahCap = resultSet.getDouble("battery_cell_capacity");
                    sapPowerMap.put(sapCode, new SAPPowerData(sapCode, kwhCap, ahCap));
                }
            } finally {
            }

            HashMap<String, TruckData> truckMap = new HashMap<>();
            try {
                ResultSet resultSet = truckPreparedStatement.executeQuery();
                while (resultSet.next()) {
                    String vin = resultSet.getString("vin");
                    Integer id = resultSet.getInt("id");
                    Integer company = resultSet.getInt("company_id");
                    Integer team = resultSet.getInt("team_id");
                    if (!team.equals(0)) {
                        truckMap.put(vin, new TruckData(id, company, team, vin));
                    }
                }
            } finally {
            }

            CapConfigBroadcastData output = new CapConfigBroadcastData(seriesStandMap, sapPowerMap, truckMap);
            sourceContext.collect(output);
            Thread.sleep(1000 * 60 * 5);
        }
    }

    @Override
    public void cancel() {
        try {
            if (connection != null) {
                connection.close();
            }
            if (seriesStandPreparedStatement != null) {
                seriesStandPreparedStatement.close();
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
