package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class Handler implements RequestHandler<Map<String,String>, String>{
    @Override
    public String handleRequest(Map<String,String> event, Context context)
    {
        /*//LambdaLogger logger = context.getLogger();
        String response = new String("This is working, 200 OK");
        LoadToMongoDB loader = new LoadToMongoDB();
        loader.loadToMongo();
        return response;*/
        LoadToSQL loadToSQL = new LoadToSQL();
        try {
            loadToSQL.readExcelData();
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static void main(String[] args) {
        Handler handler = new Handler();
        handler.handleRequest(null, null);
    }
}

