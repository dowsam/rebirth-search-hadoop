/*
 * Copyright (c) 2005-2012 www.china-cti.com All rights reserved
 * Info:rebirth-search-hadoop HdfsIndexShardGateway.java 2012-7-6 14:12:16 l.xue.nong$$
 */

package cn.com.rebirth.search.hadoop.index.gateway.hdfs;

import cn.com.rebirth.commons.settings.Settings;
import cn.com.rebirth.core.inject.Inject;
import cn.com.rebirth.core.threadpool.ThreadPool;
import cn.com.rebirth.search.core.index.gateway.IndexGateway;
import cn.com.rebirth.search.core.index.gateway.blobstore.BlobStoreIndexShardGateway;
import cn.com.rebirth.search.core.index.settings.IndexSettings;
import cn.com.rebirth.search.core.index.shard.ShardId;
import cn.com.rebirth.search.core.index.shard.service.IndexShard;
import cn.com.rebirth.search.core.index.store.Store;

/**
 * The Class HdfsIndexShardGateway.
 *
 * @author l.xue.nong
 */
public class HdfsIndexShardGateway extends BlobStoreIndexShardGateway {

    /**
     * Instantiates a new hdfs index shard gateway.
     *
     * @param shardId the shard id
     * @param indexSettings the index settings
     * @param threadPool the thread pool
     * @param hdfsIndexGateway the hdfs index gateway
     * @param indexShard the index shard
     * @param store the store
     */
    @Inject
    public HdfsIndexShardGateway(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, IndexGateway hdfsIndexGateway,
                                 IndexShard indexShard, Store store) {
        super(shardId, indexSettings, threadPool, hdfsIndexGateway, indexShard, store);
    }

    /* (non-Javadoc)
     * @see cn.com.rebirth.search.core.index.gateway.IndexShardGateway#type()
     */
    @Override
    public String type() {
        return "hdfs";
    }
}
