/*
 * Copyright (c) 2005-2012 www.china-cti.com All rights reserved
 * Info:rebirth-search-hadoop AbstractHdfsBlobContainer.java 2012-7-6 14:12:16 l.xue.nong$$
 */

package cn.com.rebirth.search.hadoop.common.blobstore.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.com.rebirth.search.commons.blobstore.BlobMetaData;
import cn.com.rebirth.search.commons.blobstore.BlobPath;
import cn.com.rebirth.search.commons.blobstore.support.AbstractBlobContainer;
import cn.com.rebirth.search.commons.blobstore.support.PlainBlobMetaData;

import com.google.common.collect.ImmutableMap;

/**
 * The Class AbstractHdfsBlobContainer.
 *
 * @author l.xue.nong
 */
public abstract class AbstractHdfsBlobContainer extends AbstractBlobContainer {

	/** The blob store. */
	protected final HdfsBlobStore blobStore;
	
	/** The logger. */
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	/** The path. */
	protected final Path path;

	/**
	 * Instantiates a new abstract hdfs blob container.
	 *
	 * @param blobStore the blob store
	 * @param blobPath the blob path
	 * @param path the path
	 */
	public AbstractHdfsBlobContainer(HdfsBlobStore blobStore, BlobPath blobPath, Path path) {
		super(blobPath);
		this.blobStore = blobStore;
		this.path = path;
	}

	/* (non-Javadoc)
	 * @see cn.com.rebirth.search.commons.blobstore.BlobContainer#listBlobs()
	 */
	public ImmutableMap<String, BlobMetaData> listBlobs() throws IOException {
		FileStatus[] files = null;
		try {
			files = blobStore.fileSystem().listStatus(path);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		if (files == null || files.length == 0) {
			return ImmutableMap.of();
		}
		ImmutableMap.Builder<String, BlobMetaData> builder = ImmutableMap.builder();
		for (FileStatus file : files) {
			builder.put(file.getPath().getName(), new PlainBlobMetaData(file.getPath().getName(), file.getLen()));
		}
		return builder.build();
	}

	/* (non-Javadoc)
	 * @see cn.com.rebirth.search.commons.blobstore.support.AbstractBlobContainer#listBlobsByPrefix(java.lang.String)
	 */
	@Override
	public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(final String blobNamePrefix) throws IOException {
		FileStatus[] files = blobStore.fileSystem().listStatus(path, new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().startsWith(blobNamePrefix);
			}
		});
		if (files == null || files.length == 0) {
			return ImmutableMap.of();
		}
		ImmutableMap.Builder<String, BlobMetaData> builder = ImmutableMap.builder();
		for (FileStatus file : files) {
			builder.put(file.getPath().getName(), new PlainBlobMetaData(file.getPath().getName(), file.getLen()));
		}
		return builder.build();
	}

	/* (non-Javadoc)
	 * @see cn.com.rebirth.search.commons.blobstore.BlobContainer#deleteBlob(java.lang.String)
	 */
	public boolean deleteBlob(String blobName) throws IOException {
		return blobStore.fileSystem().delete(new Path(path, blobName), true);
	}

	/* (non-Javadoc)
	 * @see cn.com.rebirth.search.commons.blobstore.BlobContainer#blobExists(java.lang.String)
	 */
	@Override
	public boolean blobExists(String blobName) {
		try {
			return blobStore.fileSystem().exists(new Path(path, blobName));
		} catch (IOException e) {
			return false;
		}
	}

	/* (non-Javadoc)
	 * @see cn.com.rebirth.search.commons.blobstore.BlobContainer#readBlob(java.lang.String, cn.com.rebirth.search.commons.blobstore.BlobContainer.ReadBlobListener)
	 */
	@Override
	public void readBlob(final String blobName, final ReadBlobListener listener) {
		blobStore.executor().execute(new Runnable() {
			@Override
			public void run() {
				byte[] buffer = new byte[blobStore.bufferSizeInBytes()];

				FSDataInputStream fileStream;
				try {
					fileStream = blobStore.fileSystem().open(new Path(path, blobName));
				} catch (IOException e) {
					listener.onFailure(e);
					return;
				}
				try {
					int bytesRead;
					while ((bytesRead = fileStream.read(buffer)) != -1) {
						listener.onPartial(buffer, 0, bytesRead);
					}
					listener.onCompleted();
				} catch (Exception e) {
					try {
						fileStream.close();
					} catch (IOException e1) {
						// ignore
					}
					listener.onFailure(e);
				}
			}
		});
	}
}