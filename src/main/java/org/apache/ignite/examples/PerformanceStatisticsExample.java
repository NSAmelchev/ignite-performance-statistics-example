/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsMBeanImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.PerformanceStatisticsMBean;
import org.apache.ignite.transactions.Transaction;

/**
 * This example demonstrates the usage of performance statistics.
 */
public class PerformanceStatisticsExample {
    /** Workload duration. */
    private static final int DURATION = 60_000;

    /**
     * @param args Command line arguments, none required.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            PerformanceStatisticsMBean bean = statisticsMBean(ignite.name());

            bean.start();

            runCacheOperations(ignite);
            runCompute(ignite);
            runQueries(ignite);
            runTransactions(ignite);

            ignite.compute().activeTaskFutures().forEach((uuid, fut) -> fut.get());

            bean.stop();
        }
    }

    /** */
    private static void runCacheOperations(Ignite ignite) {
        ignite.compute(ignite.cluster().forLocal()).withName("CacheOperationsTask").runAsync(() -> {
            IgniteCache<Object, Object> cache1 = ignite.getOrCreateCache("atomic-cache-1");
            IgniteCache<Object, Object> cache2 = ignite.getOrCreateCache("atomic-cache-2");

            long startTime = System.currentTimeMillis();

            while (System.currentTimeMillis() - startTime < DURATION) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                Integer key = rnd.nextInt(1000);
                Integer val = rnd.nextInt();

                IgniteCache<Object, Object> cache = rnd.nextBoolean() ? cache1 : cache2;

                if (rnd.nextBoolean())
                    cache.get(key);
                else if (rnd.nextBoolean())
                    cache.put(key, val);
                else if (rnd.nextBoolean())
                    cache.remove(key);
                else if (rnd.nextBoolean())
                    cache.putAll(Collections.singletonMap(key, val));
                else if (rnd.nextBoolean())
                    cache.getAll(Collections.singleton(key));
                else if (rnd.nextBoolean())
                    cache.removeAll(Collections.singleton(key));
                else if (rnd.nextBoolean())
                    cache.getAndPut(key, val);
                else if (rnd.nextBoolean())
                    cache.getAndRemove(key);
            }
        });
    }

    /** */
    private static void runCompute(Ignite ignite) {
        long startTime = System.currentTimeMillis();

        ignite.compute(ignite.cluster().forLocal()).withName("ComputeOperationsTask").runAsync(() -> {
            while (System.currentTimeMillis() - startTime < DURATION) {
                ignite.compute(ignite.cluster().forServers()).withName("TaskExample").broadcast(() -> {
                    U.sleep(new Random().nextInt(1000));

                    return null;
                });

                ignite.compute(ignite.cluster().forServers()).withName("SlowTaskExample").broadcast(() -> {
                    U.sleep(new Random().nextInt(DURATION / 5));

                    return null;
                });
            }
        });
    }

    /** */
    private static void runTransactions(Ignite ignite) {
        long startTime = System.currentTimeMillis();

        ignite.compute(ignite.cluster().forLocal()).withName("TransactionsOperationsTask").runAsync(() -> {
            IgniteCache<Object, Object> txCache1 = ignite.getOrCreateCache(new CacheConfiguration<>("tx-cache-1")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));
            IgniteCache<Object, Object> txCache2 = ignite.getOrCreateCache(new CacheConfiguration<>("tx-cache-2")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (System.currentTimeMillis() - startTime < DURATION) {
                Integer key = rnd.nextInt(1000);
                Integer val = rnd.nextInt();

                IgniteCache<Object, Object> cache = rnd.nextBoolean() ? txCache1 : txCache2;

                try (Transaction tx = ignite.transactions().txStart()) {
                    Integer old = (Integer)cache.get(key);

                    cache.put(key, old == null ? val : old + val);

                    try {
                        U.sleep(rnd.nextInt(100));
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        // No-op.
                    }

                    if (new Random().nextInt(1000) != 0)
                        tx.commit();
                    else
                        tx.rollback();
                }
            }
        });
    }

    /** */
    private static void runQueries(Ignite ignite) {
        long startTime = System.currentTimeMillis();

        ignite.compute(ignite.cluster().forLocal()).withName("QueryOperationsTask").runAsync(() -> {
            IgniteCache<Object, Object> cache = ignite.getOrCreateCache("atomic-cache-1");

            cache.query(new SqlFieldsQuery("create table Person (id int, val varchar, primary key (id))")).getAll();

            for (int i = 0; i < 1000; i++)
                cache.query(new SqlFieldsQuery("insert into Person (id) values (?)").setArgs(i)).getAll();

            while (System.currentTimeMillis() - startTime < DURATION) {
                cache.query(new ScanQuery<>((key, val) -> true)).getAll();

                cache.query(new SqlFieldsQuery("select * from Person where id < ?")
                    .setArgs(new Random().nextInt(100))).getAll();
            }
        });
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Ignite performance statistics MBean.
     * @throws Exception If failed.
     */
    protected static PerformanceStatisticsMBean statisticsMBean(String igniteInstanceName) throws Exception {
        ObjectName mbeanName = U.makeMBeanName(igniteInstanceName, "PerformanceStatistics",
            PerformanceStatisticsMBeanImpl.class.getSimpleName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            throw new IgniteException("MBean not registered.");

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName,
            PerformanceStatisticsMBean.class, false);
    }
}
