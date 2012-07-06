/*
 * Copyright (c) 2005-2012 www.china-cti.com All rights reserved
 * Info:rebirth-search-hadoop HdfsGatewayModule.java 2012-7-6 14:12:17 l.xue.nong$$
 */

package cn.com.rebirth.search.core.gateway.hdfs;

import cn.com.rebirth.search.core.gateway.Gateway;
import cn.com.rebirth.search.core.gateway.blobstore.BlobStoreGatewayModule;

/**
 * The Class HdfsGatewayModule.
 *
 * @author l.xue.nong
 */
public class HdfsGatewayModule extends BlobStoreGatewayModule {

	/* (non-Javadoc)
	 * @see cn.com.rebirth.search.commons.inject.AbstractModule#configure()
	 */
	@Override
	protected void configure() {
		bind(Gateway.class).to(HdfsGateway.class).asEagerSingleton();
	}
}