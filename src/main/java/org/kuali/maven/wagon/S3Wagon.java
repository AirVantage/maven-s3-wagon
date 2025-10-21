/**
 * Copyright 2010-2015 The Kuali Foundation
 *
 * Licensed under the Educational Community License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.opensource.org/licenses/ecl2.php
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kuali.maven.wagon;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authentication.AuthenticationInfo;
import org.apache.maven.wagon.proxy.ProxyInfo;
import org.apache.maven.wagon.repository.Repository;
import org.apache.maven.wagon.repository.RepositoryPermissions;
import org.kuali.maven.wagon.util.SimpleFormatter;
import org.kuali.maven.wagon.auth.V2CredentialsProviderFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.S3Exception;
import java.net.URI;
import java.time.Instant;
import java.nio.file.Files;

/**
 * <p>
 * An implementation of the Maven Wagon interface that is integrated with the Amazon S3 service.
 * </p>
 *
 * <p>
 * URLs that reference the S3 service should be in the form of <code>s3://bucket.name</code>. As an example <code>s3://maven.kuali.org</code> puts files into the
 * <code>maven.kuali.org</code> bucket on the S3 service.
 * </p>
 *
 * <p>
 * This implementation uses the <code>username</code> and <code>password</code> portions of the server authentication metadata for credentials.
 * </p>
 *
 * @author Ben Hale
 * @author Jeff Caddel
 */
public class S3Wagon extends AbstractWagon implements RequestFactory {

	/**
	 * Set the system property <code>maven.wagon.protocol</code> to <code>http</code> to force the wagon to communicate over <code>http</code>. Default is <code>https</code>.
	 */
	public static final String PROTOCOL_KEY = "maven.wagon.protocol";
	public static final String HTTP = "http";
	public static final String HTTP_ENDPOINT_VALUE = "http://s3.amazonaws.com";
	public static final String HTTPS = "https";
	public static final String MIN_THREADS_KEY = "maven.wagon.threads.min";
	public static final String MAX_THREADS_KEY = "maven.wagon.threads.max";
	public static final String DIVISOR_KEY = "maven.wagon.threads.divisor";
	public static final int DEFAULT_MIN_THREAD_COUNT = 10;
	public static final int DEFAULT_MAX_THREAD_COUNT = 50;
	public static final int DEFAULT_DIVISOR = 50;
	public static final int DEFAULT_READ_TIMEOUT = 60 * 1000;
	private static final File TEMP_DIR = getCanonicalFile(System.getProperty("java.io.tmpdir"));
	private static final String TEMP_DIR_PATH = TEMP_DIR.getAbsolutePath();

    ExecutorService executor;
	SimpleFormatter formatter = new SimpleFormatter();
	int minThreads = getMinThreads();
	int maxThreads = getMaxThreads();
	int divisor = getDivisor();
	String protocol = getValue(PROTOCOL_KEY, HTTPS);
	boolean http = HTTP.equals(protocol);
	int readTimeout = DEFAULT_READ_TIMEOUT;
	ObjectCannedACL acl = null;

	private static final Logger log = LoggerFactory.getLogger(S3Wagon.class);

	String bucketName;
	String basedir;
	S3Client client;

	// content type resolution helper
	private String resolveContentType(String destination) {
		try {
			String type = Files.probeContentType(new java.io.File(destination).toPath());
			return type != null ? type : "application/octet-stream";
		} catch (Exception e) {
			return "application/octet-stream";
		}
	}

	public S3Wagon() {
		super(true);
		S3Listener listener = new S3Listener();
		super.addSessionListener(listener);
		super.addTransferListener(listener);
	}

	/*
	 * AWS SDK v2 scaffolding for incremental migration.
	 */
	protected AwsCredentialsProvider getV2CredentialsProvider(final AuthenticationInfo authenticationInfo) {
		return V2CredentialsProviderFactory.resolve(authenticationInfo);
	}

    protected S3Client buildV2S3Client(AwsCredentialsProvider provider) {
        S3ClientBuilder builder = S3Client.builder().credentialsProvider(provider);
        // Region resolution: system property -> env -> default us-east-1
        String regionProp = System.getProperty("maven.wagon.s3.region");
        if (regionProp == null || regionProp.trim().isEmpty()) {
            regionProp = System.getenv("AWS_REGION");
        }
        if (regionProp == null || regionProp.trim().isEmpty()) {
            regionProp = System.getenv("AWS_DEFAULT_REGION");
        }
        if (regionProp == null || regionProp.trim().isEmpty()) {
            regionProp = "us-east-1";
        }
        builder.region(Region.of(regionProp.trim()));

        // Optional endpoint override (eg. MinIO) via maven.wagon.s3.endpoint
        String endpointProp = System.getProperty("maven.wagon.s3.endpoint");
        if (endpointProp != null && endpointProp.trim().length() > 0) {
            builder.endpointOverride(URI.create(endpointProp.trim()));
        }
        return builder.build();
    }

    protected void validateBucket(S3Client client, String bucketName) {
		log.debug("Looking for bucket: " + bucketName);
        try {
            client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
			validatePermissions(client, bucketName);
        } catch (S3Exception e) {
			log.info("Bucket " + bucketName + " does not exist, creating");
			client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        }
    }

	/**
	 * Establish that we have enough permissions on this bucket to do what we need to do
	 *
	 * @param client S3 Client
	 * @param bucketName AWS S3 Bucket Name.
	 */
	protected void validatePermissions(S3Client client, String bucketName) {
		// This establishes our ability to list objects in this bucket
		ListObjectsV2Request zeroObjectsRequest = ListObjectsV2Request.builder()
			.bucket(bucketName)
			.maxKeys(0)
			.build();
		client.listObjectsV2(zeroObjectsRequest);

		/**
		 * The current AWS Java SDK does not appear to have a simple method for discovering what set of permissions the currently authenticated user has on a bucket. The AWS dev's
		 * suggest that you attempt to perform an operation that would fail if you don't have the permission in question. You would then use the success/failure of that attempt to
		 * establish what your permissions are. This is definitely not ideal and they are working on it, but it is not ready yet.
		 */

		// Do something simple and quick to verify that we have write permissions on this bucket
		// One way to do this would be to create an object in this bucket, and then immediately delete it
		// That seems messy, inconvenient, and lame.

	}

	protected ObjectCannedACL getAclFromRepository(Repository repository) {
		RepositoryPermissions permissions = repository.getPermissions();
		if (permissions == null) {
			return null;
		}
		String filePermissions = permissions.getFileMode();
		if (StringUtils.isBlank(filePermissions)) {
			return null;
		}
		// map to v2 ACL enum name; keep original string for later mapping
		return ObjectCannedACL.fromValue(filePermissions.trim());
	}

	protected void connectToRepository(Repository source, AuthenticationInfo auth, ProxyInfo proxy) {
		AwsCredentialsProvider v2Provider = getV2CredentialsProvider(auth);
		this.client = buildV2S3Client(v2Provider);
		this.bucketName = source.getHost();
        validateBucket(client, bucketName);
		this.basedir = getBaseDir(source);

		// If they've specified <filePermissions> in settings.xml, that always wins
		ObjectCannedACL repoAcl = getAclFromRepository(source);
		if (repoAcl != null) {
			log.info("File permissions: " + repoAcl.name());
			acl = repoAcl;
		}
	}

	@Override
	protected boolean doesRemoteResourceExist(final String resourceName) {
		try {
			HeadObjectRequest req = HeadObjectRequest.builder()
				.bucket(bucketName)
				.key(basedir + resourceName)
				.build();
			client.headObject(req);
			return true;
		} catch (S3Exception e) {
			return false;
		}
	}

	@Override
	protected void disconnectFromRepository() {
		// Nothing to do for S3
	}

	/**
	 * Pull an object out of an S3 bucket and write it to a file
	 */
	@Override
	protected void getResource(final String resourceName, final File destination, final TransferProgress progress) throws ResourceDoesNotExistException, IOException {
		// Obtain the object from S3 (v2)
		ResponseInputStream<GetObjectResponse> objectStream;
		try {
			String key = basedir + resourceName;
			GetObjectRequest req = GetObjectRequest.builder().bucket(bucketName).key(key).build();
			objectStream = client.getObject(req);
		} catch (Exception e) {
			throw new ResourceDoesNotExistException("Resource " + resourceName + " does not exist in the repository", e);
		}

		// first write the file to a temporary location
		File temporaryDestination = File.createTempFile(destination.getName() , ".tmp",destination.getParentFile());
		InputStream in = null;
		OutputStream out = null;
		try {
			in = objectStream;
			out = new TransferProgressFileOutputStream(temporaryDestination, progress);
			byte[] buffer = new byte[1024];
			int length;
			while ((length = in.read(buffer)) != -1) {
				out.write(buffer, 0, length);
			}
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(out);
		}
		// then move, to have an atomic operation to guarantee we don't have a partially downloaded file on disk
		Files
				.move(temporaryDestination.toPath(), destination.toPath(),
					java.nio.file.StandardCopyOption.REPLACE_EXISTING);
	}

	/**
	 * Is the S3 object newer than the timestamp passed in?
	 */
	@Override
	protected boolean isRemoteResourceNewer(final String resourceName, final long timestamp) {
		HeadObjectRequest req = HeadObjectRequest.builder()
			.bucket(bucketName)
			.key(basedir + resourceName)
			.build();
		try {
			Instant lastModified = client.headObject(req).lastModified();
			return lastModified.isBefore(Instant.ofEpochMilli(timestamp));
		} catch (S3Exception e) {
			return false;
		}
	}

	/**
	 * List all of the objects in a given directory
	 */
	@Override
	protected List<String> listDirectory(String directory) throws Exception {
		// info("directory=" + directory);
		if (StringUtils.isBlank(directory)) {
			directory = "";
		}
		String delimiter = "/";
		String prefix = basedir + directory;
		if (!prefix.endsWith(delimiter)) {
			prefix += delimiter;
		}
		ListObjectsV2Request request = ListObjectsV2Request.builder()
			.bucket(bucketName)
			.prefix(prefix)
			.delimiter(delimiter)
			.build();
		ListObjectsV2Response objectListing = client.listObjectsV2(request);
		// info("truncated=" + objectListing.isTruncated());
		// info("prefix=" + prefix);
		// info("basedir=" + basedir);
		List<String> fileNames = new ArrayList<String>();
		for (software.amazon.awssdk.services.s3.model.S3Object summary : objectListing.contents()) {
			// info("summary.getKey()=" + summary.getKey());
			String key = summary.key();
			String relativeKey = key.startsWith(basedir) ? key.substring(basedir.length()) : key;
			boolean add = !StringUtils.isBlank(relativeKey) && !relativeKey.equals(directory);
			if (add) {
				// info("Adding key - " + relativeKey);
				fileNames.add(relativeKey);
			}
		}
		for (software.amazon.awssdk.services.s3.model.CommonPrefix cp : objectListing.commonPrefixes()) {
			String commonPrefix = cp.prefix();
			String value = commonPrefix.startsWith(basedir) ? commonPrefix.substring(basedir.length()) : commonPrefix;
			// info("commonPrefix=" + commonPrefix);
			// info("relativeValue=" + relativeValue);
			// info("Adding common prefix - " + value);
			fileNames.add(value);
		}
		// StringBuilder sb = new StringBuilder();
		// sb.append("\n");
		// for (String fileName : fileNames) {
		// sb.append(fileName + "\n");
		// }
		// info(sb.toString());
		return fileNames;
	}

	protected void info(String msg) {
		System.out.println("[INFO] " + msg);
	}

	/**
	 * Normalize the key to our S3 object:<br>
	 * Convert <code>./css/style.css</code> into <code>/css/style.css</code><br>
	 * Convert <code>/foo/bar/../../css/style.css</code> into <code>/css/style.css</code><br>
	 *
	 * @param key S3 Key string.
	 * @return Normalized version of {@code key}.
	 */
	protected String getCanonicalKey(String key) {
		// release/./css/style.css
		String path = basedir + key;

		// /temp/release/css/style.css
		File file = getCanonicalFile(new File(TEMP_DIR, path));
		String canonical = file.getAbsolutePath();

		// release/css/style.css
		int pos = TEMP_DIR_PATH.length() + 1;
		String suffix = canonical.substring(pos);

		// Always replace backslash with forward slash just in case we are running on Windows
		String canonicalKey = suffix.replace("\\", "/");

		// Return the canonical key
		return canonicalKey;
	}

	protected static File getCanonicalFile(String path) {
		return getCanonicalFile(new File(path));
	}

	protected static File getCanonicalFile(File file) {
		try {
			return new File(file.getCanonicalPath());
		} catch (IOException e) {
			throw new IllegalArgumentException("Unexpected IO error", e);
		}
	}

	/**
	 * On S3 there are no true "directories". An S3 bucket is essentially a Hashtable of files stored by key. The integration between a traditional file system and an S3 bucket is
	 * to use the path of the file on the local file system as the key to the file in the bucket. The S3 bucket does not contain a separate key for the directory itself.
	 */
	public final void putDirectory(File sourceDir, String destinationDir) throws TransferFailedException {

		// Examine the contents of the directory
		List<PutFileContext> contexts = getPutFileContexts(sourceDir, destinationDir);
		for (PutFileContext context : contexts) {
			// Progress is tracked by the thread handler when uploading files this way
			context.setProgress(null);
		}

		// Sum the total bytes in the directory
		long bytes = sum(contexts);

		// Show what we are up to
		log.info(getUploadStartMsg(contexts.size(), bytes));

		// Store some context for the thread handler
        int threads = Math.max(1, Math.min(maxThreads, contexts.size()));
        long start = System.currentTimeMillis();
        executor = Executors.newFixedThreadPool(threads);
        try {
            List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
            FileHandler handler = new FileHandler();
            for (int i = 0; i < contexts.size(); i++) {
                final int index = i;
                final PutFileContext ctx = contexts.get(i);
                tasks.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        handler.handle(ctx);
                        return null;
                    }
                });
            }
            List<Future<Void>> futures = executor.invokeAll(tasks);
            for (Future<Void> f : futures) {
                try {
                    f.get();
                } catch (ExecutionException ee) {
                    throw new TransferFailedException("S3 upload failed", ee.getCause());
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TransferFailedException("S3 upload interrupted", ie);
        } finally {
            executor.shutdown();
        }
        long millis = System.currentTimeMillis() - start;
        long count = contexts.size();
        log.info(getUploadCompleteMsg(millis, bytes, count));
	}

	protected String getUploadCompleteMsg(long millis, long bytes, long count) {
		String rate = formatter.getRate(millis, bytes);
		String time = formatter.getTime(millis);
		StringBuilder sb = new StringBuilder();
		sb.append("Files: " + count);
		sb.append("  Time: " + time);
		sb.append("  Rate: " + rate);
		return sb.toString();
	}

	protected String getUploadStartMsg(int fileCount, long bytes) {
		StringBuilder sb = new StringBuilder();
		sb.append("Files: " + fileCount);
		sb.append("  Bytes: " + formatter.getSize(bytes));
		return sb.toString();
	}

	protected int getRequestsPerThread(int threads, int requests) {
		int requestsPerThread = requests / threads;
		while (requestsPerThread * threads < requests) {
			requestsPerThread++;
		}
		return requestsPerThread;
	}

	protected long sum(List<PutFileContext> contexts) {
		long sum = 0;
		for (PutFileContext context : contexts) {
			File file = context.getSource();
			long length = file.length();
			sum += length;
		}
		return sum;
	}

	/**
	 * Store a resource into S3
	 */
	@Override
	protected void putResource(final File source, final String destination, final TransferProgress progress) throws IOException {

		// Delegate to upload(context) which uses SDK v2
		PutFileContext ctx = getPutFileContext(source, destination);
		ctx.setProgress(progress);
		upload(ctx);
	}

	protected String getDestinationPath(final String destination) {
		return destination.substring(0, destination.lastIndexOf('/'));
	}

	/**
	 * Convert "/" -&gt; ""<br>
	 * Convert "/snapshot/" &gt; "snapshot/"<br>
	 * Convert "/snapshot" -&gt; "snapshot/"<br>
	 *
	 * @param source Repository info.
	 * @return Normalized repository base dir.
	 */
	protected String getBaseDir(final Repository source) {
		StringBuilder sb = new StringBuilder(source.getBasedir());
		sb.deleteCharAt(0);
		if (sb.length() == 0) {
			return "";
		}
		if (sb.charAt(sb.length() - 1) != '/') {
			sb.append('/');
		}
		return sb.toString();
	}

	@Override
	protected PutFileContext getPutFileContext(File source, String destination) {
		PutFileContext context = super.getPutFileContext(source, destination);
		context.setFactory(this);
		context.setClient(this.client);
		return context;
	}

	protected int getMinThreads() {
		return getValue(MIN_THREADS_KEY, DEFAULT_MIN_THREAD_COUNT);
	}

	protected int getMaxThreads() {
		return getValue(MAX_THREADS_KEY, DEFAULT_MAX_THREAD_COUNT);
	}

	protected int getDivisor() {
		return getValue(DIVISOR_KEY, DEFAULT_DIVISOR);
	}

	protected int getValue(String key, int defaultValue) {
		String value = System.getProperty(key);
		if (StringUtils.isEmpty(value)) {
			return defaultValue;
		} else {
			return new Integer(value);
		}
	}

	protected String getValue(String key, String defaultValue) {
		String value = System.getProperty(key);
		if (StringUtils.isEmpty(value)) {
			return defaultValue;
		} else {
			return value;
		}
	}

	public int getReadTimeout() {
		return readTimeout;
	}

	public void setReadTimeout(int readTimeout) {
		this.readTimeout = readTimeout;
	}

	// RequestFactory implementation using AWS SDK v2 for uploads
	@Override
	public void upload(PutFileContext context) throws IOException {
		final File source = context.getSource();
		final String destination = context.getDestination();
		final TransferProgress progress = context.getProgress();
		final String key = getCanonicalKey(destination);

		software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder req =
			software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
				.bucket(bucketName)
				.key(key)
				.contentLength(source.length())
				.contentType(resolveContentType(destination));

		if (acl != null) {
			req.acl(acl);
		}

		if (progress == null) {
			context.getClient().putObject(req.build(), software.amazon.awssdk.core.sync.RequestBody.fromFile(source.toPath()));
		} else {
			// When progress is requested, stream through our TransferProgress wrapper
			try (InputStream in = new TransferProgressFileInputStream(source, progress)) {
				context.getClient().putObject(req.build(), software.amazon.awssdk.core.sync.RequestBody.fromInputStream(in, source.length()));
			}
		}
	}

}
