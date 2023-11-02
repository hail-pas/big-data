package cn.burnish.bigdata.traceroute.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.*;

public class MySQLSourceFunction<T> implements SourceFunction<T> {

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String query;
    private final RowMapper<T> rowMapper;

    private volatile boolean isRunning = true;

    public MySQLSourceFunction(String jdbcUrl, String username, String password, String query, RowMapper<T> rowMapper) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.query = query;
        this.rowMapper = rowMapper;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
        PreparedStatement statement = connection.prepareStatement(query);

        while (isRunning) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                T record = rowMapper.mapRow(resultSet);
                if (record != null) ctx.collect(record);
            }

            // Sleep or add some other logic for controlling the rate of data emission
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public interface RowMapper<T> {
        T mapRow(ResultSet resultSet) throws Exception;
    }
}
