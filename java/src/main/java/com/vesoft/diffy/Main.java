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
        System.out.println();
        System.out.println(run());
        System.out.println();
    }

    public static String run() throws Exception {
        Config     cfg        = new Config();
        Controller controller = new Controller(cfg);
        controller.execute();
        long latencyUs  = controller.getAvgLatencyUs();
        long responseUs = controller.getAvgResponseUs();
        long decodeUs = controller.getAvgDecodeUs();

        Response response = new Response(latencyUs, responseUs, decodeUs);
        return JSON.toJSONString(response);
    }

    static class Controller {
        private Config config;
        private Output output;

        public Controller(Config config) {
            this.config = config;
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
                    .withRequestTimeoutMills(10000)
                    .build();

            ExecutorService      executorService = Executors.newFixedThreadPool(config.getConcurrency());
            List<Future<Output>> futures         = new ArrayList<>();

            for (int i = 0; i < config.getConcurrency(); i++) {
                futures.add(executorService.submit(() -> runWorker(pool)));
            }

            long totalLatencyUs  = 0;
            long totalResponseUs = 0;
            long totalDecodeUs = 0;

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
                for (int i = 0; i < config.getIterationsPerConcurrency(); i++) {
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
        long avgLatencyUs;
        long avgResponseUs;
        long avgDecodeUs;

        public Response(long avgLatencyUs, long avgResponseUs, long avgDecodeUs) {
            this.avgLatencyUs = avgLatencyUs;
            this.avgResponseUs = avgResponseUs;
            this.avgDecodeUs = avgDecodeUs;
        }

        public long getAvgLatencyUs() {
            return avgLatencyUs;
        }

        public void setAvgLatencyUs(long avgLatencyUs) {
            this.avgLatencyUs = avgLatencyUs;
        }

        public long getAvgResponseUs() {
            return avgResponseUs;
        }

        public void setAvgResponseUs(long avgResponseUs) {
            this.avgResponseUs = avgResponseUs;
        }

        public long getAvgDecodeUs() {
            return avgDecodeUs;
        }

        public void setAvgDecodeUs(long avgDecodeUs) {
            this.avgDecodeUs = avgDecodeUs;
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
