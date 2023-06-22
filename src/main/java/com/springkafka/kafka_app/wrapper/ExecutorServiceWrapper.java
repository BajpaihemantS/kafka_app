package com.springkafka.kafka_app.wrapper;

import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class ExecutorServiceWrapper {

    private ExecutorService executorService;

    public void setThreadCount(int threadCount) {
//        System.out.println("setThreadCount reaches");
        executorService = Executors.newFixedThreadPool(threadCount);
//        System.out.println("setThreadCount ends");
    }

    public void submit(Runnable task) {
        executorService.submit(task);
    }

    public void stop() {

        executorService.shutdownNow();

        while (!executorService.isTerminated()){
            executorService.shutdownNow();
        }
    }


}


//refactor
//wrapping up of executor service
//singletonclass
