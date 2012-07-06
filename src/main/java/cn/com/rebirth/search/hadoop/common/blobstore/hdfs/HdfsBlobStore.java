/*
 * Copyright (c) 2005-2012 www.china-cti.com All rights reserved
 * Info:rebirth-search-hadoop HdfsBlobStore.java 2012-7-6 14:12:16 l.xue.nong$$
 */

package cn.com.rebirth.search.hadoop.common.blobstore.hdfs;

import java.io.IOException;
import java.util.concurrent.Executor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.com.rebirth.commons.settings.Settings;
import cn.com.rebirth.commons.unit.ByteSizeUnit;
import cn.com.rebirth.commons.unit.ByteSizeValue;
import cn.com.rebirth.search.commons.blobstore.BlobPath;
import cn.com.rebirth.search.commons.blobstore.BlobStore;
import cn.com.rebirth.search.commons.blobstore.ImmutableBlobContainer;

/**
 * The Class HdfsBlobStore.
 *
 * @author l.xue.nong
 */
public class HdfsBlobStore implements BlobStore {

	/** The file system. */
	private final FileSystem fileSystem;

	/** The path. */
	private final Path path;

	/** The executor. */
	private final Executor executor;

	/** The buffer size in bytes. */
	private final int bufferSizeInBytes;

	/**
	 * Instantiates a new hdfs blob store.
	 *
	 * @param settings the settings
	 * @param fileSystem the file system
	 * @param executor the executor
	 * @param path the path
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public HdfsBlobStore(Settings settings, FileSystem fileSystem, Executor executor, Path path) throws IOException {
		this.fileSystem = fileSystem;
		this.path = path;

		if (!fileSystem.exists(path)) {
			fileSystem.mkdirs(path);
		}

		this.bufferSizeInBytes = (int) settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB))
				.bytes();
		this.executor = executor;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return path.toString();
	}

	/**
	 * Buffer size in bytes.
	 *
	 * @return the int
	 */
	public int bufferSizeInBytes() {
		return this.bufferSizeInBytes;
	}

	/**
	 * File system.
	 *
	 * @return the file system
	 */
	public FileSystem fileSystem() {
		return fileSystem;
	}

	/**
	 * Path.
	 *
	 * @return the path
	 */
	public Path path() {
		return path;
	}

	/**
	 * Executor.
	 *
	 * @return the executor
	 */
	public Executor executor() {
		return executor;
	}

	/* (non-Javadoc)
	 * @see cn.com.rebirth.search.commons.blobstore.BlobStore#immutableBlobContainer(cn.com.rebirth.search.commons.blobstore.BlobPath)
	 */
	@Override
	public ImmutableBlobContainer immutableBlobContainer(BlobPath path) {
		return new HdfsImmutableBlobContainer(this, path, buildAndCreate(path));
	}

	/* (non-Javadoc)
	 * @see cn.com.rebirth.search.commons.blobstore.BlobStore#delete(cn.com.rebirth.search.commons.blobstore.BlobPath)
	 */
	@Override
	public void delete(BlobPath path) {
		try {
			fileSystem.delete(buildPath(path), true);
		} catch (IOException e) {
			// ignore
		}
	}

	/* (non-Javadoc)
	 * @see cn.com.rebirth.search.commons.blobstore.BlobStore#close()
	 */
	@Override
	public void close() {
	}

	/**
	 * Builds the and create.
	 *
	 * @param blobPath the blob path
	 * @return the path
	 */
	private Path buildAndCreate(BlobPath blobPath) {
		Path path = buildPath(blobPath);
		try {
			fileSystem.mkdirs(path);
		} catch (IOException e) {
			// ignore
		}
		return path;
	}

	/**
	 * Builds the path.
	 *
	 * @param blobPath the blob path
	 * @return the path
	 */
	private Path buildPath(BlobPath blobPath) {
		Path path = this.path;
		for (String p : blobPath) {
			path = new Path(path, p);
		}
		return path;
	}
}
