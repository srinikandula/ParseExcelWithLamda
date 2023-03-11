package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.util.Map;

public class Handler implements RequestHandler<Map<String,String>, String>{
    @Override
    public String handleRequest(Map<String,String> event, Context context)
    {
        //LambdaLogger logger = context.getLogger();
        String response = new String("This is working, 200 OK");
        LoadToMongoDB loader = new LoadToMongoDB();
        loader.loadToMongo();
        return response;
    }

    public static void main(String[] args) {
        Handler handler = new Handler();
        handler.handleRequest(null, null);
    }
}

