/*
 * Copyright (c) 2005-2012 www.china-cti.com All rights reserved
 * Info:rebirth-search-hadoop HdfsImmutableBlobContainer.java 2012-7-6 14:12:16 l.xue.nong$$
 */

package cn.com.rebirth.search.hadoop.common.blobstore.hdfs;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import cn.com.rebirth.search.commons.blobstore.BlobPath;
import cn.com.rebirth.search.commons.blobstore.ImmutableBlobContainer;
import cn.com.rebirth.search.commons.blobstore.support.BlobStores;

/**
 * The Class HdfsImmutableBlobContainer.
 *
 * @author l.xue.nong
 */
public class HdfsImmutableBlobContainer extends AbstractHdfsBlobContainer implements ImmutableBlobContainer {

	/**
	 * Instantiates a new hdfs immutable blob container.
	 *
	 * @param blobStore the blob store
	 * @param blobPath the blob path
	 * @param path the path
	 */
	public HdfsImmutableBlobContainer(HdfsBlobStore blobStore, BlobPath blobPath, Path path) {
		super(blobStore, blobPath, path);
	}

	@Override
	public void writeBlob(final String blobName, final InputStream is, final long sizeInBytes,
			final WriterListener listener) {
		blobStore.executor().execute(new Runnable() {
			@Override
			public void run() {
				Path file = new Path(path, blobName);

				FSDataOutputStream fileStream;
				try {
					fileStream = blobStore.fileSystem().create(file, true);
				} catch (IOException e) {
					listener.onFailure(e);
					return;
				}
				try {
					try {
						byte[] buffer = new byte[blobStore.bufferSizeInBytes()];
						int bytesRead;
						while ((bytesRead = is.read(buffer)) != -1) {
							fileStream.write(buffer, 0, bytesRead);
						}
					} finally {
						try {
							is.close();
						} catch (IOException ex) {
							// do nothing
						}
						try {
							fileStream.close();
						} catch (IOException ex) {
							// do nothing
						}
					}
					listener.onCompleted();
				} catch (Exception e) {
					// just on the safe size, try and delete it on failure
					try {
						if (blobStore.fileSystem().exists(file)) {
							blobStore.fileSystem().delete(file, true);
						}
					} catch (Exception e1) {
						// ignore
					}
					listener.onFailure(e);
				}
			}
		});
	}

	@Override
	public void writeBlob(String blobName, InputStream is, long sizeInBytes) throws IOException {
		BlobStores.syncWriteBlob(this, blobName, is, sizeInBytes);
	}
}