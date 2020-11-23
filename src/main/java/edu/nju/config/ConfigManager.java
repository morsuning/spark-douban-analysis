package edu.nju.config;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * @author xuechenyang(morsuning @ gmail.com)
 * @date 2020/11/21 22:28
 */
public class ConfigManager implements Serializable {
    /**
     * 私有配置对象
     */
    private static final Properties PROP = new Properties();

    static {
        try {
            //获取配置文件输入流
            InputStream in = ConfigManager.class
                    .getClassLoader().getResourceAsStream("demo.properties");

            //加载配置对象
            PROP.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取指定key对应的value
     *
     * @param key
     * @return value
     */
    public static String getProperty(String key) {
        return PROP.getProperty(key);
    }

    /**
     * 获取整数类型的配置项
     *
     * @param key
     * @return value
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取布尔类型的配置项
     *
     * @param key
     * @return value
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取Long类型的配置项
     *
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }
}
