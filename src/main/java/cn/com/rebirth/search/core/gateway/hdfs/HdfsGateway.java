/*
 * Copyright (c) 2005-2012 www.china-cti.com All rights reserved
 * Info:rebirth-search-hadoop HdfsGateway.java 2012-7-6 14:12:17 l.xue.nong$$
 */

package cn.com.rebirth.search.core.gateway.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.com.rebirth.commons.concurrent.EsExecutors;
import cn.com.rebirth.commons.exception.RebirthException;
import cn.com.rebirth.commons.exception.RebirthIllegalArgumentException;
import cn.com.rebirth.commons.settings.Settings;
import cn.com.rebirth.core.inject.Inject;
import cn.com.rebirth.core.inject.Module;
import cn.com.rebirth.core.threadpool.ThreadPool;
import cn.com.rebirth.search.core.cluster.ClusterName;
import cn.com.rebirth.search.core.cluster.ClusterService;
import cn.com.rebirth.search.core.gateway.blobstore.BlobStoreGateway;
import cn.com.rebirth.search.hadoop.common.blobstore.hdfs.HdfsBlobStore;

/**
 * The Class HdfsGateway.
 *
 * @author l.xue.nong
 */
public class HdfsGateway extends BlobStoreGateway {

	/** The close file system. */
	private final boolean closeFileSystem;

	/** The file system. */
	private final FileSystem fileSystem;

	/** The concurrent stream pool. */
	private final ExecutorService concurrentStreamPool;

	/**
	 * Instantiates a new hdfs gateway.
	 *
	 * @param settings the settings
	 * @param threadPool the thread pool
	 * @param clusterService the cluster service
	 * @param clusterName the cluster name
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Inject
	public HdfsGateway(Settings settings, ThreadPool threadPool, ClusterService clusterService, ClusterName clusterName)
			throws IOException {
		super(settings, threadPool, clusterService);

		this.closeFileSystem = componentSettings.getAsBoolean("close_fs", true);
		String uri = componentSettings.get("uri");
		if (uri == null) {
			throw new RebirthIllegalArgumentException("hdfs gateway requires the 'uri' setting to be set");
		}
		String path = componentSettings.get("path");
		if (path == null) {
			throw new RebirthIllegalArgumentException("hdfs gateway requires the 'path' path setting to be set");
		}
		Path hPath = new Path(new Path(path), clusterName.value());

		int concurrentStreams = componentSettings.getAsInt("concurrent_streams", 5);
		this.concurrentStreamPool = EsExecutors.newScalingExecutorService(1, concurrentStreams, 5, TimeUnit.SECONDS,
				EsExecutors.daemonThreadFactory(settings, "[s3_stream]"));

		logger.debug("Using uri [{}], path [{}], concurrent_streams [" + concurrentStreams + "]", uri, hPath);

		Configuration conf = new Configuration();
		Settings hdfsSettings = settings.getByPrefix("hdfs.conf.");
		for (Map.Entry<String, String> entry : hdfsSettings.getAsMap().entrySet()) {
			conf.set(entry.getKey(), entry.getValue());
		}

		fileSystem = FileSystem.get(URI.create(uri), conf);

		initialize(new HdfsBlobStore(settings, fileSystem, concurrentStreamPool, hPath), clusterName, null);
	}

	@Override
	public String type() {
		return "hdfs";
	}

	@Override
	public Class<? extends Module> suggestIndexGateway() {
		return HdfsIndexGatewayModule.class;
	}

	@Override
	protected void doClose() throws RebirthException {
		super.doClose();
		if (closeFileSystem) {
			try {
				fileSystem.close();
			} catch (IOException e) {
				// ignore
			}
		}
		concurrentStreamPool.shutdown();
	}
}
