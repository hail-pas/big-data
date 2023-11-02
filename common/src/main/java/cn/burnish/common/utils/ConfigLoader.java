package cn.burnish.common.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Properties;

public class ConfigLoader {
    public static Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    public static String ENVIRONMENT = "development";

    /**
     * 通过命令行参数获取配置
     *
     * @param parameterTool 配置信息
     */
    public static void getConfig(ParameterTool parameterTool) throws Exception {
        Properties properties = parameterTool.getProperties();

        ENVIRONMENT = (String) properties.get("environment");

        logger.info("===================加载配置====================");
        String className = Thread.currentThread().getStackTrace()[2].getClassName();
        for (Field fd : Class.forName(className).getDeclaredFields()) {
            String f_name = fd.getName();
            if (f_name.startsWith("config")) {
                String value = (String) properties.get(f_name);
                if (value == null) {
                    throw new Exception(String.format("缺少配置项: %s", f_name));
                } else {
                    Class<?> type = fd.getType();
                    if (type == String.class) {
                        fd.set(value, value);
                    } else if (type == long.class || type == Long.class) {
                        Long fullValue = Long.parseLong(value);
                        fd.set(fullValue, fullValue);
                    } else if (type == int.class || type == Integer.class) {
                        Integer fullValue = Integer.parseInt(value);
                        fd.set(fullValue, fullValue);
                    }
                    logger.info(String.format("%-24s: %s", f_name, value));
                }
            }
        }
        logger.info("===================加载配置完成==================");
    }
}