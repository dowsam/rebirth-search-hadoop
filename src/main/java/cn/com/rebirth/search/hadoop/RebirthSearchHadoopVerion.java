/*
 * Copyright (c) 2005-2012 www.china-cti.com All rights reserved
 * Info:rebirth-search-hadoop RebirthSearchHadoopVerion.java 2012-7-6 14:12:16 l.xue.nong$$
 */
package cn.com.rebirth.search.hadoop;

import cn.com.rebirth.commons.Version;

/**
 * The Class RebirthSearchHadoopVerion.
 *
 * @author l.xue.nong
 */
public class RebirthSearchHadoopVerion implements Version {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -8043854155386241988L;

	@Override
	public String getModuleVersion() {
		return "0.0.1.RC1-SNAPSHOT";
	}

	@Override
	public String getModuleName() {
		return "rebirth-search-hadoop";
	}

}
