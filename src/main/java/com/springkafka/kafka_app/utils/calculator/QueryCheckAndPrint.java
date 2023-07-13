package com.springkafka.kafka_app.utils.calculator;

import com.springkafka.kafka_app.utils.Query.Attribute;
import com.springkafka.kafka_app.utils.Query.AttributeType;
import com.springkafka.kafka_app.utils.Query.Query;
import com.springkafka.kafka_app.wrapper.CustomLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

public class QueryCheckAndPrint extends CustomLogger {

    public static boolean checkQuery(Map<String,Long> userAttributeCount, Query query, long eventTime) {

        long startTime = query.getTimestamp().getStartTime();
        long endTime = query.getTimestamp().getEndTime();

        boolean timeCheck = eventTime>=startTime && eventTime<=endTime;

        for(AttributeType attributeType : query.getAttributeTypeList()){
            for(Attribute attribute : attributeType.getAttributeList()){
                String attributeName = attribute.getValue();
                if(attribute.getCount()==null){
                    continue;
                }
                Long countValue = attribute.getCount().getValue();
                String countRelation = attribute.getCount().getRelation();

                Long eventTypeCount = userAttributeCount.get(attributeName);

                switch (countRelation) {
                    case "exact" -> {
                        if (!Objects.equals(eventTypeCount, countValue)) {
                            return false;
                        }
                    }
                    case "gte" -> {
                        if (eventTypeCount < countValue) {
                            return false;
                        }
                    }
                    case "lte" -> {
                        if (eventTypeCount > countValue) {
                            return false;
                        }
                    }
                    case "gt" -> {
                        if (eventTypeCount <= countValue) {
                            return false;
                        }
                    }
                    case "lt" -> {
                        if (eventTypeCount >= countValue) {
                            return false;
                        }
                    }
                    default -> throw new IllegalArgumentException("The relation given is not valid" + countRelation);
                }
            }
        }

        return timeCheck;
    }

    public static void printUsers(HashSet<String> userSet){
        Logger logger = LoggerFactory.getLogger(org.slf4j.Logger.class);
        for(String user : userSet){
            logger.info(user);
        }
    }
}
