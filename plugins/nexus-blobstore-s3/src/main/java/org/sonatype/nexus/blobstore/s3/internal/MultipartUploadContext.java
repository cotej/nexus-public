/*
 * Sonatype Nexus (TM) Open Source Version
 * Copyright (c) 2008-present Sonatype, Inc.
 * All rights reserved. Includes the third-party code listed at http://links.sonatype.com/products/nexus/oss/attributions.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License Version 1.0,
 * which accompanies this distribution and is available at http://www.eclipse.org/legal/epl-v10.html.
 *
 * Sonatype Nexus (TM) Professional Version is available from Sonatype, Inc. "Sonatype" and "Sonatype Nexus" are trademarks
 * of Sonatype, Inc. Apache Maven is a trademark of the Apache Software Foundation. M2eclipse is a trademark of the
 * Eclipse Foundation. All other trademarks are the property of their respective owners.
 */
package org.sonatype.nexus.blobstore.s3.internal;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Helper class to {@link MultipartUploader} used to facilitate and track state for an asynchronous multipart upload.
 *
 * @since 3.17
 */
class MultipartUploadContext {

  private final AmazonS3 s3;
  private final String uploadId;
  private final Semaphore permit;

  private final List<CompletableFuture<UploadPartResult>> parts = new ArrayList<>();
  private final AtomicReference<Exception> error = new AtomicReference<>();

  MultipartUploadContext(final AmazonS3 s3, final String uploadId, final int concurrency) {
    this.s3 = s3;
    this.uploadId = uploadId;
    this.permit = new Semaphore(concurrency, true);
  }

  void uploadPartAsync(final UploadPartRequest part) throws IOException {
    try {
      permit.acquire();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while queueing parts for upload", e);
    }

    parts.add(CompletableFuture.supplyAsync(() -> {
      try {
        if (!hasError()) {
          return s3.uploadPart(part);
        }
      } catch (Exception e) {
        error.set(e);
      } finally {
        permit.release();
      }
      return null;
    }));
  }

  List<UploadPartResult> getUploadResults() throws IOException {
    List<UploadPartResult> results = new ArrayList<>();
    for (CompletableFuture<UploadPartResult> part : parts) {
      results.add(part.join());
    }
    if (hasError()) {
      throw new IOException("Upload " + uploadId + " has failed", error.get());
    }
    return results;
  }

  boolean hasError() {
    return error.get() != null;
  }
}
