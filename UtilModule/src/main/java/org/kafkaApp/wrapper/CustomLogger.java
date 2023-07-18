package org.kafkaApp.wrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This was the wrapper of the logger class
 * which describes the function which were required by me.
 */

public abstract class CustomLogger {
    static Logger logger = LoggerFactory.getLogger(org.slf4j.Logger.class);

    public static void debug(String message, Object... args) {
        logger.debug(message, args);
    }

    public static void info(Object... args) {
        logger.info("",args);
    }
    public static void info(String message, Object... args) {
        logger.info(message, args);
    }


    public static void error(String message, Throwable throwable) {
        logger.error(message, throwable);
    }


}