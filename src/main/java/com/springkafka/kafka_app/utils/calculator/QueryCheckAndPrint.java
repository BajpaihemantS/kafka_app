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

        System.out.println("---111---");


        for(AttributeType attributeType : query.getAttributeTypeList()){
            for(Attribute attribute : attributeType.getAttributeList()){
                String attributeName = attribute.getValue();
                System.out.println("---333---");
                if(attribute.getCount()==null){
                    continue;
                }
                System.out.println("---444---");
                Long countValue = attribute.getCount().getValue();
                System.out.println("---4.14.14.1---");
                String countRelation = attribute.getCount().getRelation();
                System.out.println("---4.24.24.2--- " + attributeName);

                Long eventTypeCount;
                if(userAttributeCount.containsKey(attributeName)){
                    eventTypeCount = userAttributeCount.get(attributeName);
                }
                else{
                    eventTypeCount = 1L;
                }
                System.out.println(eventTypeCount);
                System.out.println("---555----");

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
                            System.out.println("the count required is " + countValue + " and the count given is " + eventTypeCount);
                            return false;
                        }
                    }
                    default -> throw new IllegalArgumentException("The relation given is not valid" + countRelation);
                }
            }
        }

        System.out.println("---222---" + timeCheck);
        return timeCheck;
    }

}
