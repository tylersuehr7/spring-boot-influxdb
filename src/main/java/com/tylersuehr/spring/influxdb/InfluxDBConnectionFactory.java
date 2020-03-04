/*
 * Copyright (c) Tyler R. Suehr 2020. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tylersuehr.spring.influxdb;

import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A factory for creating connections to Influx database.
 * @author Tyler Suehr
 */
public class InfluxDBConnectionFactory implements InitializingBean {
    private static final Logger logger
            = LoggerFactory.getLogger(InfluxDBConnectionFactory.class);

    private InfluxDB connection;
    private InfluxDBProperties properties;

    /**
     * Default constructor.
     */
    public InfluxDBConnectionFactory() {}

    /**
     * Constructs with the specified database properties.
     * @param props the properties
     */
    public InfluxDBConnectionFactory(InfluxDBProperties props) {
        this.properties = props;
        getConnection();
    }

    /**
     * Lazily fetches the database connection.
     * @return the connection
     */
    public InfluxDB getConnection() {
        if (connection == null) {
            final InfluxDBProperties props = getProperties();
            final OkHttpClient.Builder client = new OkHttpClient.Builder()
                    .connectTimeout(props.getConnectTimeout(), TimeUnit.SECONDS)
                    .writeTimeout(props.getWriteTimeout(), TimeUnit.SECONDS)
                    .readTimeout(props.getReadTimeout(), TimeUnit.SECONDS);
            this.connection = InfluxDBFactory.connect(props.getUrl(), props.getUsername(), props.getPassword(), client);
            if (props.isGzip()) {
                this.connection.enableGzip();
            }
            logger.debug("Create InfluxDB connection");
        }
        return connection;
    }

    /**
     * Gets the currently configured database properties.
     * @return the properties
     */
    public InfluxDBProperties getProperties() {
        return properties;
    }

    /**
     * Sets the currently configured database properties.
     * @param props the properties
     */
    public void setProperties(InfluxDBProperties props) {
        this.properties = props;
    }

    @Override
    public void afterPropertiesSet() {
        Objects.requireNonNull(getProperties(), "InfluxDB properties must be specified!");
    }
}
