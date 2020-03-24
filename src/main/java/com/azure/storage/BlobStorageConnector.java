package com.azure.storage;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.file.datalake.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;

public class BlobStorageConnector {

    private static final String FORWARD_SLASH = "/";
    private static final String UNDER_SCORE = "_";

    public static void main(String[] args) throws Exception {

        DataLakeServiceClient dataLakeServiceClient = getDataLakeServiceClient("<storage_account_name>",
                "<client_id>", "<client_secret>>", "<tenant_id>");
        Instant currentInstant = Instant.now();
        int year = currentInstant.atZone(ZoneOffset.UTC).getYear();
        DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient("<container_name>");
        StringBuilder adls2FilePath = new StringBuilder();
        adls2FilePath.append(year).append(FORWARD_SLASH).append("testdir");
        DataLakeDirectoryClient dataLakeDirectoryClient = dataLakeFileSystemClient.createDirectory(adls2FilePath.toString());
        String fileNamePrefix = String.valueOf(Instant.now().toEpochMilli());
        byte[] uploadedFileBytes = "test".getBytes();
        InputStream inputStream = new ByteArrayInputStream(uploadedFileBytes);
        DataLakeFileClient dataLakeFileClient = dataLakeDirectoryClient.createFile(UNDER_SCORE + fileNamePrefix + UNDER_SCORE + "test.txt");
        dataLakeFileClient.append(inputStream,0,uploadedFileBytes.length);
        dataLakeFileClient.flush(uploadedFileBytes.length);

    }

    static public DataLakeServiceClient getDataLakeServiceClient
            (String accountName, String clientId, String ClientSecret, String tenantID){
        String endpoint = "https://" + accountName + ".dfs.core.windows.net";

        ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()
                .clientId(clientId)
                .clientSecret(ClientSecret)
                .tenantId(tenantID)
                .build();

        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();
        return builder.credential(clientSecretCredential).endpoint(endpoint).buildClient();
    }
}
