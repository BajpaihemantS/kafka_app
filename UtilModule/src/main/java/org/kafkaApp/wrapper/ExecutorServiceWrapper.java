package org.kafkaApp.wrapper;

import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This was the wrapper for the Executors service
 * which describes the functions required by me.
 */

@Service
public class ExecutorServiceWrapper {

    private ExecutorService executorService;

    public void setThreadCount(int threadCount) {
        executorService = Executors.newFixedThreadPool(threadCount);
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
