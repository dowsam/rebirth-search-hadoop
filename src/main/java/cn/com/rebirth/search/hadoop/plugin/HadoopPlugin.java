/*
 * Copyright (c) 2005-2012 www.china-cti.com All rights reserved
 * Info:rebirth-search-hadoop HadoopPlugin.java 2012-7-6 14:12:16 l.xue.nong$$
 */

package cn.com.rebirth.search.hadoop.plugin;

import cn.com.rebirth.search.core.plugins.AbstractPlugin;

/**
 * The Class HadoopPlugin.
 *
 * @author l.xue.nong
 */
public class HadoopPlugin extends AbstractPlugin {

	/* (non-Javadoc)
	 * @see cn.com.rebirth.search.core.plugins.Plugin#name()
	 */
	@Override
	public String name() {
		return "hadoop";
	}

	/* (non-Javadoc)
	 * @see cn.com.rebirth.search.core.plugins.Plugin#description()
	 */
	@Override
	public String description() {
		return "Hadoop Plugin";
	}
}
