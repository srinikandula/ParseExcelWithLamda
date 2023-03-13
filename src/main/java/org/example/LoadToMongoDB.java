package org.example;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.bson.Document;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class LoadToMongoDB {
    public static String COLLECTION_NAME = "clientData";
    public void loadToMongo(){
        // Creating a Mongo client
        MongoClient mongo = new MongoClient( "localhost" , 27017 );

        // Creating Credentials
        MongoCredential credential = MongoCredential.createCredential("sampleUser", "myDb", "password".toCharArray());
        System.out.println("Connected to the database successfully");

        //Accessing the database
        MongoDatabase database = mongo.getDatabase("myDb");

        //Creating a collection
        MongoCollection collection = database.getCollection(COLLECTION_NAME);
        try{
            database.createCollection(COLLECTION_NAME);
        }catch (Exception e) {
           // e.printStackTrace();
        }
        readExcelData(database,COLLECTION_NAME);
        /*Document document = new Document();
        document.append("name", "Ram");
        document.append("age", 26);
        document.append("city", "Hyderabad");
        database.getCollection(COLLECTION_NAME).insertOne(document);*/
        System.out.println("DONE");
    }

    private void readExcelData(MongoDatabase database,String COLLECTION_NAME){
        // Try block to check for exceptions
        ArrayList<String> columns = new ArrayList<>();
        try {
            // Reading file from local directory
            FileInputStream file = new FileInputStream(
                    new File("C:\\Users\\KALYANI\\Documents\\excel1.xlsx"));
            // Create Workbook instance holding reference to
            // .xlsx file
            XSSFWorkbook workbook = new XSSFWorkbook(file);
            // Get first/desired sheet from the workbook
            XSSFSheet sheet = workbook.getSheetAt(0);
            // Iterate through each rows one by one
            Iterator<Row> rowIterator = sheet.iterator();
            int rowVal = 1;
            // Till there is an element condition holds true
            while (rowIterator.hasNext()) {
                Document document = new Document();
                ArrayList<String> cellValues = new ArrayList<>();
                Row row = rowIterator.next();
                // For each row, iterate through all the columns
                Iterator<Cell> cellIterator
                        = row.cellIterator();
                FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
                    // Checking the cell type and format accordingly
                    switch (evaluator.evaluateInCell(cell).getCellType()) {
                        // Case 1
                        case NUMERIC:
                            if(rowVal == 1){
                                columns.add(String.valueOf(cell.getNumericCellValue()));
                            }else{
                                cellValues.add(String.valueOf(cell.getNumericCellValue()));
                            }
                            break;
                        // Case 2
                        case STRING:
                            if(rowVal == 1){
                                columns.add(cell.getStringCellValue());
                            }else{
                                cellValues.add(cell.getStringCellValue());
                            }
                            break;
                    }
                }
                if(rowVal > 1){
                    for(int i=0;i<columns.size();i++){
                        document.append(columns.get(i), cellValues.get(i));
                    }
                    database.getCollection(COLLECTION_NAME).insertOne(document);
                }
                rowVal ++;
            }
            // Closing file output streams
            file.close();
        }
        // Catch block to handle exceptions
        catch (Exception e) {
            // Display the exception along with line number
            // using printStackTrace() method
            e.printStackTrace();
        }
    }

}
