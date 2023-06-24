package com.springkafka.kafka_app.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CustomLogger {
    Logger logger = LoggerFactory.getLogger(org.slf4j.Logger.class);

    public void debug(String message, Object... args) {
        logger.debug(message, args);
    }

    public void info(String message, Object... args) {
        logger.info(message, args);
    }

    public void error(String message, Throwable throwable) {
        logger.error(message, throwable);
    }


}