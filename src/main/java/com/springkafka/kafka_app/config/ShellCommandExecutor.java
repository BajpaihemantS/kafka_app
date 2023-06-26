package com.springkafka.kafka_app.config;

import com.springkafka.kafka_app.utils.CustomLogger;

import java.io.IOException;

public class ShellCommandExecutor extends CustomLogger {
    public void runZookeeper() {
        try {
            String command1 = "cd Downloads/kafka_2.12-3.5.0";

            Process process1 = Runtime.getRuntime().exec(command1);
            int exitCode1 = process1.waitFor();

            if (exitCode1 == 0) {
                System.out.println("Command 1 executed successfully.");

                String command2 = "./bin/zookeeper-server-start.sh ./config/zookeeper.properties";

                Process process2 = Runtime.getRuntime().exec(command2);
                int exitCode2 = process2.waitFor();

                if (exitCode2 == 0) {
                    info("Zookeeper running");
                } else {
                    info("Command 2 execution failed with exit code: {}", exitCode2);
                }
            } else {
               info("Command 1 execution failed with exit code: {} ", exitCode1);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
