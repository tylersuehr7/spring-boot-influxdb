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

import org.influxdb.InfluxDB;
import org.influxdb.dto.*;
import org.springframework.beans.factory.InitializingBean;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Simplifies the usage of InfluxDb and makes it more object-oriented.
 * @author Tyler Suehr
 */
public class InfluxDBTemplate<T> implements InitializingBean {
    private final InfluxDB connection;
    private final InfluxDBProperties properties;
    private Converter<T> converter;

    /**
     * Constructs with the InfluxDb connection factory
     * @param factory the connection factory
     */
    public InfluxDBTemplate(InfluxDBConnectionFactory factory) {
        this.connection = factory.getConnection();
        this.properties = factory.getProperties();
        createDatabase(properties.getDatabase());
    }

    /**
     * Gets the established connection for the InfluxDb.
     * @return the connection
     */
    public InfluxDB getConnection() {
        return connection;
    }

    /**
     * Gets the configured properties for teh InfluxDb connection.
     * @return the properties
     */
    public InfluxDBProperties getProperties() {
        return properties;
    }

    public Converter<T> getConverter() {
        return converter;
    }

    public void setConverter(Converter<T> converter) {
        this.converter = converter;
    }

    /**
     * Creates a database with the specified name.
     * @param name the name
     */
    public void createDatabase(String name) {
        final Query query = new Query("CREATE DATABASE " + name);
        this.connection.query(query);
    }

    /**
     * Deletes a database with the specified name.
     * @param name the name
     */
    public void deleteDatabase(String name) {
        final Query query = new Query("DROP DATABASE " + name);
        this.connection.query(query);
    }

    /**
     * Determines if a database exists with a specified name.
     *
     * @param name the name
     * @return true if exists, otherwise false
     */
    public boolean databaseExists(String name) {
        return connection.databaseExists(name);
    }

    /**
     * Writes a single object to the database.
     * @param model the object
     */
    public void write(T model) {
        final InfluxDBProperties props = this.properties;
        final String database = props.getDatabase();
        final String retentionPolicy = props.getRetentionPolicy();
        this.connection.write(database, retentionPolicy, converter.convert(model));
    }

    /**
     * Writes an array of objects to the database.
     * @param models the objects
     */
    public void write(T[] models) {
        write(Arrays.asList(models));
    }

    /**
     * Writes a collection of objects to the database.
     * @param models the collection of objects
     */
    public void write(Collection<T> models) {
        final InfluxDBProperties props = this.properties;
        final String database = props.getDatabase();
        final String retentionPolicy = props.getRetentionPolicy();
        final BatchPoints ops = BatchPoints.database(database)
                .retentionPolicy(retentionPolicy)
                .consistency(InfluxDB.ConsistencyLevel.ALL)
                .build();
        models.forEach(m -> ops.point(converter.convert(m)));
        this.connection.write(ops);
    }

    /**
     * Performs a query on the database.
     *
     * @param query the query
     * @return the result
     */
    public QueryResult query(Query query) {
        return this.connection.query(query);
    }

    /**
     * Performs a query on the database.
     *
     * @param query the query
     * @param timeUnit the time unit of result
     * @return the result
     */
    public QueryResult query(Query query, TimeUnit timeUnit) {
        return this.connection.query(query, timeUnit);
    }

    /**
     * Performs a query on the database.
     *
     * @param query the query
     * @param chunkSize the size of each result chunk
     * @param consumer the callback
     */
    public void query(Query query, int chunkSize, Consumer<QueryResult> consumer) {
        this.connection.query(query, chunkSize, consumer);
    }

    public Pong ping() {
        return connection.ping();
    }

    public String version() {
        return connection.version();
    }

    @Override
    public void afterPropertiesSet() {
        Objects.requireNonNull(getConnection(), "InfluxDb connection cannot be null!");
        Objects.requireNonNull(getProperties(), "InfluxDb properties cannot be null!");
        Objects.requireNonNull(getConverter(), "InfluxDb model converter cannot be null!");
    }

    /**
     * Defines a model adapter for the InfluxDb.
     * @param <T> the type of model
     */
    public interface Converter<T> {
        /**
         * Called to convert a model into an insertable point.
         *
         * @param model the model to be converted
         * @return the point
         */
        Point convert(T model);
    }
}
