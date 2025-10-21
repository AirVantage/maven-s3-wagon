package org.kuali.maven.wagon.auth;

import org.apache.maven.wagon.authentication.AuthenticationInfo;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Factory to produce AWS SDK v2 credentials providers based on Maven AuthenticationInfo
 * or default chain (supports SSO, profiles, web identity, IMDS/ECS, env, etc.).
 */
public final class V2CredentialsProviderFactory {

	private V2CredentialsProviderFactory() {}

	public static AwsCredentialsProvider resolve(AuthenticationInfo auth) {
		if (auth != null && auth.getUserName() != null && auth.getPassword() != null) {
			String accessKey = auth.getUserName();
			String secretKey = auth.getPassword();
			String sessionToken = auth.getPassphrase(); // optional: store session token in passphrase
			if (sessionToken != null && !sessionToken.isEmpty()) {
				return StaticCredentialsProvider.create(AwsSessionCredentials.create(accessKey, secretKey, sessionToken));
			} else {
				return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
			}
		}
		return DefaultCredentialsProvider.create();
	}
}


