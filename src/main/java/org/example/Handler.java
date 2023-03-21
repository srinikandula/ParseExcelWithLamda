package org.example;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

public class Handler implements RequestHandler<S3Event, String>{
    @Override
    public String handleRequest(com.amazonaws.services.lambda.runtime.events.S3Event event, Context context) {
        LambdaLogger logger = context.getLogger();
        S3EventNotification.S3EventNotificationRecord record = event.getRecords().get(0);
        String s3Bucket = record.getS3().getBucket().getName();
        String filePath = record.getS3().getObject().getKey();
        String awsAccessKey = "";
        String awsSecretKey = "";
        logger.log("got file "+ filePath +" from bucket " + s3Bucket);
        AWSCredentials credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion("ap-south-1")
                .withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
        S3Object result = s3Client.getObject(s3Bucket,filePath);
        S3ObjectInputStream data = result.getObjectContent();
        try {
            InputStream stream = new ByteArrayInputStream(IOUtils.toByteArray(data));
            LoadToSQL loadToSQL = new LoadToSQL();
            logger.log("calling the handler now");
            loadToSQL.readExcelData(stream, logger);
            logger.log("calling the handler END");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
        String response = new String("This is working, 200 OK");
        return response;
    }

    public static void main(String[] args) throws SQLException, IOException {
        LoadToSQL loadToSQL = new LoadToSQL();
        FileInputStream fis = new FileInputStream("/Users/srinikandula/Downloads/4lakhs_records.xlsx");
        loadToSQL.readExcelData(fis, null);
    }
}

