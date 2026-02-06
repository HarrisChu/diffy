package com.vesoft.diffy;

import com.vesoft.nebula.driver.graph.data.ResultSet;
import com.vesoft.nebula.driver.graph.net.NebulaClient;
import com.vesoft.nebula.driver.graph.net.NebulaPool;
import com.alibaba.fastjson.JSON;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        boolean warmup = false;
        if (args.length > 0 && args[0].equalsIgnoreCase("warmup")) {
            warmup = true;
        }
        System.out.println();
        System.out.println(run(warmup));
        System.out.println();
    }

    public static String run(boolean warmup) throws Exception {
        Config     cfg        = new Config();
        Controller controller = new Controller(cfg);
        if (warmup) {
            controller.warmup();
        }
        controller.execute();
        long latencyUs  = controller.getAvgLatencyUs();
        long responseUs = controller.getAvgResponseUs();
        long decodeUs   = controller.getAvgDecodeUs();

        Response response = new Response(latencyUs, responseUs, decodeUs);
        return JSON.toJSONString(response);
    }

    static class Controller {
        private Config config;
        private Output output;
        private int    iterEachConcurrency;

        public Controller(Config config) {
            this.config = config;
            this.iterEachConcurrency = config.getIterationsPerConcurrency();
        }

        public void warmup() throws Exception {
            NebulaPool pool = null;
            try {
                pool = NebulaPool.builder(config.getAddress(), config.getUser(), config.getPassword())
                        .withMaxClientSize(1)
                        .withConnectTimeoutMills(5000)
                        .withRequestTimeoutMills(60000)
                        .build();
                iterEachConcurrency = config.getWarnup();
                runWorker(pool);
                iterEachConcurrency = config.getIterationsPerConcurrency();
            } finally {
                if (pool != null) {
                    pool.close();
                }
            }

        }

        public void execute() throws Exception {
            output = run();
        }

        public long getAvgLatencyUs() throws Exception {
            return output.totalLatencyUs / (config.getConcurrency() * config.getIterationsPerConcurrency());
        }

        public long getAvgResponseUs() throws Exception {
            return output.totalResponseUs / (config.getConcurrency() * config.getIterationsPerConcurrency());
        }

        public long getAvgDecodeUs() throws Exception {
            return output.totalDecodeUs / (config.getConcurrency() * config.getIterationsPerConcurrency());
        }

        private Output run() throws Exception {
            NebulaPool pool = NebulaPool.builder(config.getAddress(), config.getUser(), config.getPassword())
                    .withMaxClientSize(config.getConcurrency())
                    .withConnectTimeoutMills(5000)
                    .withRequestTimeoutMills(60000)
                    .build();

            ExecutorService      executorService = Executors.newFixedThreadPool(config.getConcurrency());
            List<Future<Output>> futures         = new ArrayList<>();

            for (int i = 0; i < config.getConcurrency(); i++) {
                futures.add(executorService.submit(() -> runWorker(pool)));
            }

            long totalLatencyUs  = 0;
            long totalResponseUs = 0;
            long totalDecodeUs   = 0;

            for (Future<Output> future : futures) {
                Output output = future.get();
                totalLatencyUs += output.totalLatencyUs;
                totalResponseUs += output.totalResponseUs;
                totalDecodeUs += output.totalDecodeUs;
            }

            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.MINUTES);
            pool.close();

            return new Output(totalLatencyUs, totalResponseUs, totalDecodeUs);
        }

        private Output runWorker(NebulaPool pool) throws Exception {
            long totalLatencyUs  = 0;
            long totalResponseUs = 0;
            long totalDecodeUs   = 0;

            NebulaClient client          = null;
            long         startTime       = 0;
            long         endTime         = 0;
            long         startDecodeTime = 0;
            try {
                client = pool.getClient();
                startTime = System.nanoTime();
                for (int i = 0; i < iterEachConcurrency; i++) {
                    ResultSet result = client.execute(config.getStatement());
                    startDecodeTime = System.nanoTime();
                    while (result.hasNext()) {
                        result.next();
                    }
                    totalDecodeUs += (System.nanoTime() - startDecodeTime) / 1000;
                    totalLatencyUs += result.getLatency();
                }
            } finally {
                if (client != null) {
                    pool.returnClient(client);
                }
            }
            endTime = System.nanoTime();
            totalResponseUs += (endTime - startTime) / 1000;
            return new Output(totalLatencyUs, totalResponseUs, totalDecodeUs);
        }

        private List<HostAddress> parseAddress(String address) {
            List<HostAddress> addresses = new ArrayList<>();
            String[]          parts     = address.split(",");
            for (String part : parts) {
                String[] hostPort = part.trim().split(":");
                if (hostPort.length == 2) {
                    addresses.add(new HostAddress(hostPort[0], Integer.parseInt(hostPort[1])));
                }
            }
            return addresses;
        }
    }

    static class Output {
        long totalLatencyUs;
        long totalResponseUs;
        long totalDecodeUs;

        public Output(long totalLatencyUs, long totalResponseUs, long totalDecodeUs) {
            this.totalLatencyUs = totalLatencyUs;
            this.totalResponseUs = totalResponseUs;
            this.totalDecodeUs = totalDecodeUs;
        }
    }

    static class Response {
        long avg_latency_us;
        long avg_response_us;
        long avg_decode_us;

        public Response(long avgLatencyUs, long avgResponseUs, long avgDecodeUs) {
            this.avg_latency_us = avgLatencyUs;
            this.avg_response_us = avgResponseUs;
            this.avg_decode_us = avgDecodeUs;
        }

        public long getAvg_latency_us() {
            return avg_latency_us;
        }

        public void setAvg_latency_us(long avg_latency_us) {
            this.avg_latency_us = avg_latency_us;
        }

        public long getAvg_response_us() {
            return avg_response_us;
        }

        public void setAvg_response_us(long avg_response_us) {
            this.avg_response_us = avg_response_us;
        }

        public long getAvg_decode_us() {
            return avg_decode_us;
        }

        public void setAvg_decode_us(long avg_decode_us) {
            this.avg_decode_us = avg_decode_us;
        }
    }


    static class HostAddress {
        String host;
        int    port;

        public HostAddress(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }
    }
}
