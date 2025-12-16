package teamdb.classes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
//import DBAppException;

public class Serializer {
     // Replace with your desired folder name
    private static String extension = ".class";
    private static String filePath = "src/Serializations/";

    public static void serialize(Object obj, String strFileName) throws IOException{   //obj = page     strFileName = esm el page
        try {
        	
			/*if(Serializer.wasSerialized(strFileName)){
			throw new DBAppException("Table already exists");
			}*/	
         FileOutputStream fileOut = new FileOutputStream(filePath+strFileName+extension);
         ObjectOutputStream out = new ObjectOutputStream(fileOut);
         out.writeObject(obj);
         out.close();
         fileOut.close();
      } catch (IOException i) {
         i.printStackTrace();
      }
    }

    public static Object deSerialize(String strFileName) throws Exception {
        if(!Serializer.wasSerialized(strFileName)){
            //TODO add Dbappexc
			throw new IOException(strFileName+" is not serialized");
		}
		File f = new File(filePath + strFileName + extension);
		FileInputStream fis = new FileInputStream(f);
		@SuppressWarnings("resource")
        ObjectInputStream ois = new ObjectInputStream(fis);		
		Object ret = ois.readObject();
		return ret;
	}

   public static boolean wasSerialized(String strFileName) {
      String fileName = strFileName + extension;
      File directory = new File(filePath);
      if (directory.isDirectory()) {
          File[] files = directory.listFiles();
          if (files != null) {
              for (File file : files) {
                  if (file.getName().equals(fileName)) {
                      return true;
                  }
              }
          } else {
              System.out.println("Unable to access directory contents.");
              return false;
          }
      } else {
          return false;
      }
      return false;
  }
}