package org.example;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;

public class LoadToSQL {

    public void loadToSQLDemoMethod() throws SQLException {
        String sqlSelectAllPersons = "SELECT * FROM persons";
        String connectionUrl = "jdbc:mysql://localhost:3306/mydb?serverTimezone=UTC";
        try (Connection conn = DriverManager.getConnection(connectionUrl, "root", "Ammuloki@3030");
             PreparedStatement ps = conn.prepareStatement(sqlSelectAllPersons);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                long id = rs.getLong("ID");
                String lastName = rs.getString("LastName");
                String firstName = rs.getString("FirstName");
                // do something with the extracted data...
                System.out.println(firstName+" : "+lastName);
            }
        } catch (SQLException e) {
            // handle the exception
            e.printStackTrace();
        }
    }

    public void readExcelData() throws IOException, SQLException {
        String connectionUrl = "jdbc:mysql://localhost:3306/mydb?serverTimezone=UTC";
        Connection conn = DriverManager.getConnection(connectionUrl, "root", "Ammuloki@3030");
        Statement stmt = conn.createStatement();
        String sqlCreateStmt = "CREATE TABLE Students (Name varchar(255),Age int,Place varchar(255));" ;
        stmt.executeUpdate(sqlCreateStmt);
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
                ArrayList<String> cellValues = new ArrayList<>();
                Row row = rowIterator.next();
                String cellValuesStr = "";
                String insertQuery;
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
                            cellValues.add(String.valueOf(cell.getNumericCellValue()));
                            break;
                        // Case 2
                        case STRING:
                            cellValues.add("'"+cell.getStringCellValue()+"'");
                            break;
                    }
                }
                //insert each row into table
                if(rowVal != 1){
                    for(int i=0;i<cellValues.size();i++){
                        if(i == cellValues.size()-1){
                            cellValuesStr += cellValues.get(i);
                        }else{
                            cellValuesStr += cellValues.get(i)+",";
                        }

                    }
                    insertQuery = "INSERT INTO Students (Name,Age,Place) VALUES ("+cellValuesStr+");";
                    stmt.executeUpdate(insertQuery);
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
