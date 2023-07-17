package com.springkafka.kafka_app.utils.calculator;

import com.springkafka.kafka_app.wrapper.CustomLogger;
import com.springkafka.kafka_app.utils.Query.Attribute;
import com.springkafka.kafka_app.utils.Query.AttributeType;
import com.springkafka.kafka_app.utils.Query.Query;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class QueryCheckAndPrintUsers extends CustomLogger {

    public static boolean checkQuery(Map<String,Integer> userAttributeCount, Query query) {

        for(AttributeType attributeType : query.getAttributeTypeList()){
            for(Attribute attribute : attributeType.getAttributeList()){
                String attributeName = attribute.getValue();
                if(attribute.getCount()==null){
                    continue;
                }
                Integer countValue = attribute.getCount().getValue();
                String countRelation = attribute.getCount().getRelation();

                Integer eventTypeCount = userAttributeCount.get(attributeName);

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

        return true;
    }

    public static void printUsers(HashMap<String,Long> userMap, Query query){

        long queryStartTime = query.getTimestamp().getStartTime();
        long queryEndTime = query.getTimestamp().getEndTime();

        userMap.forEach((user,eventTime) -> {
            boolean timeCheck = eventTime>=queryStartTime && eventTime<=queryEndTime;
            if(timeCheck){
                info(user);
            }
        });

    }
}
