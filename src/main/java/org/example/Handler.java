package org.example;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.xspec.L;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class Handler implements RequestHandler<S3Event, String>{
    @Override
    public String handleRequest(com.amazonaws.services.lambda.runtime.events.S3Event event, Context context) {
        LambdaLogger logger = context.getLogger();
        S3EventNotification.S3EventNotificationRecord record = event.getRecords().get(0);
        String s3Bucket = record.getS3().getBucket().getName();
        String filePath = record.getS3().getObject().getKey();
        String awsAccessKey = "AKIAY6C7CZZXHLLDM7X7";
        String awsSecretKey = "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";
        logger.log("got file "+ filePath +" from bucket " + s3Bucket);
        AWSCredentials credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion("ap-south-1")
                .withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
        S3Object result = s3Client.getObject(s3Bucket,filePath);
        S3ObjectInputStream data = result.getObjectContent();
        try {
            InputStream stream = new ByteArrayInputStream(IOUtils.toByteArray(data));
            LoadToMongoDB loadToMongoDB = new LoadToMongoDB();
            loadToMongoDB.loadToMongo(stream, logger);
            logger.log("read from file now");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String response = new String("This is working, 200 OK");
        return response;
    }

    public static void main(String[] args) {
        Handler handler = new Handler();
        handler.handleRequest(null, null);
    }
}

