package org.example;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFName;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoadToSQL {

    private DataSource ds;

    public void loadToSQLDemoMethod() throws SQLException {
        String sqlSelectAllPersons = "SELECT * FROM persons";
        String connectionUrl = "jdbc:mysql://anyaudit-in.crccd5mpnjb5.ap-south-1.rds.amazonaws.com:3306/mydb?serverTimezone=UTC";
        try (Connection conn = DriverManager.getConnection(connectionUrl, "root", "SadhgurooPaahi^^9");
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

    public void readExcelData(InputStream inputStream, LambdaLogger logger) throws IOException, SQLException {
        if(logger != null) {
            logger.log("parsing excel now : ");
        }
        ds = initiateDataSource();
        List<List<String>> colummValuesList = new ArrayList<>();
        String pstmtString = null;
        String tableName = "sampleData";
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(20);
            XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
            List<XSSFName>  names = workbook.getAllNames();
            // Get first/desired sheet from the workbook
            XSSFSheet sheet = workbook.getSheetAt(0);
            // Iterate through each rows one by one
            Iterator<Row> rowIterator = sheet.iterator();
            int rowVal = 1;
            StringBuilder columnNames = new StringBuilder();
            StringBuilder pstmtParams = new StringBuilder();
            // Till there is an element condition holds true
            while (rowIterator.hasNext()) {
                ArrayList<String> cellValues = new ArrayList<>();
                Row row = rowIterator.next();
                // For each row, iterate through all the columns
                Iterator<Cell> cellIterator = row.cellIterator();
                FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
                if(rowVal == 1) {
                    StringBuilder createQuery = new StringBuilder();
                    do{
                        Cell cell = cellIterator.next();
                        String cellValue = cell.getStringCellValue();
                        if(cellValue != null && cellValue.trim().length() > 0){
                            if(createQuery.length() != 0) {
                                createQuery.append(",");
                                columnNames.append(",");
                                pstmtParams.append(",");
                            }
                            createQuery.append(cell.getStringCellValue() +" varchar(255)");
                            columnNames.append(cell.getStringCellValue());
                            pstmtParams.append("?");
                        }

                    }while (cellIterator.hasNext());
                    String sqlCreateStmt = "CREATE TABLE "+tableName+" ("+createQuery+");";
                    Connection conn = ds.getConnection();
                    Statement stmt = conn.createStatement();
                    int result = stmt.executeUpdate(sqlCreateStmt);
                    if(logger != null) {
                        logger.log("create command: "+ sqlCreateStmt +" result:"+ result);
                    }
                    stmt.close();
                    conn.close();
                    pstmtString = String.format("insert into %s (%s) values(%s)", tableName, columnNames, pstmtParams);
                } else {
                    List<String> columnValues = new ArrayList<>();;
                    do {
                        Cell cell = cellIterator.next();
                        switch (evaluator.evaluateInCell(cell).getCellType()) {
                            // Case 1
                            case NUMERIC:
                                columnValues.add(String.valueOf(cell.getNumericCellValue()));
                                break;
                            // Case 2
                            case STRING:
                                columnValues.add(cell.getStringCellValue());
                                break;
                        }
                    }while (cellIterator.hasNext());
                    colummValuesList.add(columnValues);
                }
                rowVal ++;
                if(colummValuesList.size() == 500) {
                    List<List<String>> tempList = new ArrayList<>();
                    tempList.addAll(colummValuesList);
                    Runnable t = new MySQLBatchInsertThread(tempList, ds, pstmtString);
                    executorService.submit(t);
                    colummValuesList.clear();
                }
            }
            //Do not ignore smaller excel
            if(colummValuesList.size() > 0) {
                List<List<String>> tempList = new ArrayList<>();
                tempList.addAll(colummValuesList);
                Thread t = new Thread(new MySQLBatchInsertThread(tempList, ds, pstmtString));
                executorService.submit(t);
            }
            executorService.shutdown();
            // Closing file output streams
            inputStream.close();
        }
        // Catch block to handle exceptions
        catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }

    private DataSource initiateDataSource() {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass("com.mysql.jdbc.Driver");
            cpds.setJdbcUrl("jdbc:mysql://anyaudit-in..ap-south-1.rds.amazonaws.com:3306/tmp?serverTimezone=UTC");
            cpds.setUser("root");
            cpds.setPassword("");
            cpds.setMinPoolSize(20);
            cpds.setMaxPoolSize(40);
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }
        return cpds;
    }

}
