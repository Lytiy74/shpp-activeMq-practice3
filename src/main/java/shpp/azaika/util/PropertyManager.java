package shpp.azaika.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyManager {
    private final Properties properties;
    private static final Logger logger = LoggerFactory.getLogger(PropertyManager.class);


    public PropertyManager(String propertyFileName) throws IOException {
        logger.debug("Loading properties from {}", propertyFileName);
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(propertyFileName);
        this.properties = new Properties();
        this.properties.load(inputStream);
    }

    public String getProperty(String key) {
        return this.properties.getProperty(key);
    }

}
