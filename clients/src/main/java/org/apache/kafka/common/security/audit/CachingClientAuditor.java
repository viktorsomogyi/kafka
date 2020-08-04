package org.apache.kafka.common.security.audit;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CachingClientAuditor {

    private static final Logger LOG = LoggerFactory.getLogger(CachingClientAuditor.class);

    private long producerCacheExpiryMs = 10000;
    private int producerCacheMaxSize = 100000;

    private CacheLoader<String, Set<TopicPartition>> cacheLoader = new CacheLoader<String, Set<TopicPartition>>() {
        @Override
        public Set<TopicPartition> load(String key) {
            return ConcurrentHashMap.newKeySet();
        }
    };
    private RemovalListener<String, Set<TopicPartition>> removalListener = notification -> {
        LOG.debug("Removing client {} from {}", notification.getKey(), notification.getCause());
    };

    private LoadingCache<String, Set<TopicPartition>> producerCache = Caffeine.newBuilder()
        .expireAfterWrite(producerCacheExpiryMs, TimeUnit.MILLISECONDS)
        .expireAfterAccess(producerCacheExpiryMs, TimeUnit.MILLISECONDS)
        .maximumSize(producerCacheMaxSize)
        .initialCapacity(producerCacheMaxSize/2)
        .removalListener(removalListener)
        .build(cacheLoader);
}
