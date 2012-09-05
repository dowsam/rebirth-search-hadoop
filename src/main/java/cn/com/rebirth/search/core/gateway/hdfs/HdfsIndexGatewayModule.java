/*
 * Copyright (c) 2005-2012 www.china-cti.com All rights reserved
 * Info:rebirth-search-hadoop HdfsIndexGatewayModule.java 2012-7-6 14:12:16 l.xue.nong$$
 */

package cn.com.rebirth.search.core.gateway.hdfs;

import cn.com.rebirth.core.inject.AbstractModule;
import cn.com.rebirth.search.core.index.gateway.IndexGateway;
import cn.com.rebirth.search.hadoop.index.gateway.hdfs.HdfsIndexGateway;

/**
 * The Class HdfsIndexGatewayModule.
 *
 * @author l.xue.nong
 */
public class HdfsIndexGatewayModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(IndexGateway.class).to(HdfsIndexGateway.class).asEagerSingleton();
	}
}