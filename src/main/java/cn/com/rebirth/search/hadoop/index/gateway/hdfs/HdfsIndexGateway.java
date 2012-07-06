/*
 * Copyright (c) 2005-2012 www.china-cti.com All rights reserved
 * Info:rebirth-search-hadoop HdfsIndexGateway.java 2012-7-6 14:12:17 l.xue.nong$$
 */

package cn.com.rebirth.search.hadoop.index.gateway.hdfs;

import cn.com.rebirth.commons.settings.Settings;
import cn.com.rebirth.search.commons.inject.Inject;
import cn.com.rebirth.search.core.gateway.Gateway;
import cn.com.rebirth.search.core.index.Index;
import cn.com.rebirth.search.core.index.gateway.IndexShardGateway;
import cn.com.rebirth.search.core.index.gateway.blobstore.BlobStoreIndexGateway;
import cn.com.rebirth.search.core.index.settings.IndexSettings;

/**
 * The Class HdfsIndexGateway.
 *
 * @author l.xue.nong
 */
public class HdfsIndexGateway extends BlobStoreIndexGateway {

	/**
	 * Instantiates a new hdfs index gateway.
	 *
	 * @param index the index
	 * @param indexSettings the index settings
	 * @param gateway the gateway
	 */
	@Inject
	public HdfsIndexGateway(Index index, @IndexSettings Settings indexSettings, Gateway gateway) {
		super(index, indexSettings, gateway);
	}

	/* (non-Javadoc)
	 * @see cn.com.rebirth.search.core.index.gateway.IndexGateway#type()
	 */
	@Override
	public String type() {
		return "hdfs";
	}

	/* (non-Javadoc)
	 * @see cn.com.rebirth.search.core.index.gateway.IndexGateway#shardGatewayClass()
	 */
	@Override
	public Class<? extends IndexShardGateway> shardGatewayClass() {
		return HdfsIndexShardGateway.class;
	}
}
