package teamdb.classes;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigReader {
    private int maxRows;
    private String fileFull = "src/resources/DBApp.config";

    public ConfigReader() {
        Properties properties = new Properties();

        try (FileInputStream input = new FileInputStream(fileFull)) {
            // Load the properties from the input stream
            properties.load(input);

            // Retrieve the value for a specific key
            maxRows = Integer.parseInt(properties.getProperty("MaximumRowsCountinPage"));
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getMaxRows(){
        return maxRows;
    }
}

