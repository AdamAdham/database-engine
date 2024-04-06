package teamdb.classes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;
import java.io.Serializable;

public class Table implements Serializable,Cloneable{
    private String tableName;
    private String clusteringKeyColumn;
    private Hashtable<String, String> colNameType;  // Column name to data type mapping
    private Vector<String> pageFileNames;  // Vector of Page objects
    private Hashtable<String,Index> indexHash;

    public Table(String tableName, String clusteringKeyColumn, Hashtable<String, String> colNameType) throws DBAppException {
        this.tableName = tableName;
        this.clusteringKeyColumn = clusteringKeyColumn;
        this.colNameType = colNameType;
        this.pageFileNames = new Vector<String>();
        this.indexHash = new Hashtable<String,Index>();
        try {
            Serializer.serialize(this, tableName);
        } catch (IOException e) {
            System.out.println("Error While Creating Table");
        }
    }

    public Page createPage() throws Exception {
        String newPageName;
		int newIndex = 1;
		for (int i = (pageFileNames.size() - 1); i >= 0; i--) { //
            // TODO : Change double single etc digits
			  if (pageFileNames.get(i).contains(tableName)) {
				  newIndex = Character.getNumericValue(pageFileNames.get(i).charAt(pageFileNames.get(i).length() -1)) + 1; //6 characters for .class, 1 character for normal string indexing
				  break;
			  }
		}
		newPageName = tableName + newIndex;
        Page ret = new Page(newPageName);
		pageFileNames.add(newPageName);
		return ret;
	}

    public Hashtable<String, String> getColNameType(){
        return colNameType;
    }

    public String getClusteringKeyColumn (){
        return clusteringKeyColumn;
    }

    public String getTableName(){
        return tableName;
    }

    public Hashtable<String,Index> getIndexHash(){
        return indexHash;
    }

    public Vector<String> getPageFileNames(){
        return pageFileNames;
    }

    public void addIndexHash(String indexName,Index index){
        indexHash.put(indexName, index);
    }

    public String toString() {
		String str = "";
		for(int i = 0; i < pageFileNames.size(); i++) {
			try {
				str += ((Page)Serializer.deSerialize(pageFileNames.get(i))).toString() + "\n";
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return str;
	}

    public void setClusterKey(String key) {
		this.clusteringKeyColumn = key;
	}

    public void removemtpage(Page p) throws DBAppException {
	    if (p.isEmpty()) {
	        pageFileNames.remove(p.fileName);
	        try {
	            Serializer.serialize(this, tableName);
	        } catch (IOException e) {
	            System.out.println("Error While Updating Table After Removing Empty Pages");
	        }
	    } else {
	        System.out.println("Page is not empty, cannot remove.");
	    }
	}

}
