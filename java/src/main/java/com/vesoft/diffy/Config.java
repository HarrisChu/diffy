/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.diffy;

public class Config {
    private String address;
    private String user;
    private String password;
    private String statement;
    private int    concurrency;
    private int    iterationsPerConcurrency;
    private int    warnup;

    public Config() {
        this.address = getEnv("NEBULA_ADDRESS", "192.168.15.11:9669");
        this.user = getEnv("NEBULA_USER", "harris");
        this.password = getEnv("NEBULA_PASSWORD", "Nebula2025.harris");
        this.statement = getEnv("NEBULA_STATEMENT", "use sf1 match p=(v:Comment)->(v1) return p order by v.id desc  limit 10");
        this.concurrency = getEnvAsInt("NEBULA_CONCURRENCY", 1);
        this.iterationsPerConcurrency = getEnvAsInt("NEBULA_ITERATIONS_PER_CONCURRENCY", 10);
        this.warnup = getEnvAsInt("WARM_UP", 1);
    }

    private String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    private int getEnvAsInt(String name, int defaultVal) {
        String valueStr = System.getenv(name);
        if (valueStr != null) {
            try {
                return Integer.parseInt(valueStr);
            } catch (NumberFormatException e) {
                return defaultVal;
            }
        }
        return defaultVal;
    }

    public String getAddress() {
        return address;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getStatement() {
        return statement;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public int getIterationsPerConcurrency() {
        return iterationsPerConcurrency;
    }

    public int getWarnup() {
        return warnup;
    }
}
