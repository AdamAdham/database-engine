package teamdb.classes;

import java.io.IOException;
import java.security.Key;
import java.util.Vector;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.io.Serializable;

public class Index implements Serializable{
    private String name;
    private String tableName;
    private String colName;
    private String clusterColumn; // To eliminate the necessity of deserializing table
    private bplustree bTree;

    public Index(String tableName,String colName,String indexName,String clusterColumn){
        this.name = indexName;
        this.tableName = tableName; 
        this.colName = colName;
        this.bTree = new bplustree(4);
        this.clusterColumn = clusterColumn;
        try {
            Serializer.serialize(this, indexName);
        } catch (IOException e) {
            // TODO Add DBAppException
            System.out.println("Error While Creating Table");
        }
    }

    public String getName(){
        return name;
    }

    public String getTableName(){
        return tableName;
    }

    public String getColName(){
        return colName;
    }

    public bplustree getBTree(){
        return bTree;
    }

    public HashMap<Page,Vector<Integer>> selectEqual(String columnName,Comparable<Key> k) throws Exception{
        HashMap<Page,Vector<Integer>> result = new HashMap<Page,Vector<Integer>>();
        Vector<String> pageNames = bTree.search(k);
        if(pageNames.size()==0){
            // Btree does not contain value
            return result;
        }
        for(int i=0;i<pageNames.size();i++){
            Page page;
            try {
                page = (Page) Serializer.deSerialize(pageNames.get(i));
            } catch (Exception e) {
                // TODO: handle exception
                throw new Exception("Deserializer Error");
            }
            Vector<Integer> indices;
            if(columnName==clusterColumn){
                // only one because it is clustered by this column
                indices = new Vector<Integer>();
                indices.add(AdamHelpers.binarySearchPageAdam(page,columnName,k));
            }else{
                indices = AdamHelpers.linearSearchPageAdam(page.getTuples(),columnName,k);
            }
            if(indices.size()==0){
                // Dont know how this will happen since the index points at that page so i think index is broken
                // TODO Change this
                System.out.println(name + " Index has incorrect pointer to page");
            }else{
                result.put(page, indices); // Add index to array list for that page
            }
        }
        return result;
    }

    public Vector<Tuple> selectEqualTuples(String columnName,Comparable<Key> k) throws Exception{
        Vector<Tuple> result = new Vector<Tuple>();
        Vector<String> pageNames = bTree.search(k);
        if(pageNames.size()==0){
            // Btree does not contain value
            return result;
        }
        for(int i=0;i<pageNames.size();i++){
            Page page;
            try {
                page = (Page) Serializer.deSerialize(pageNames.get(i));
            } catch (Exception e) {
                // TODO: handle exception
                throw new Exception("Deserializer Error");
            }
            Vector<Tuple> tuples = page.getTuples();
            Vector<Integer> indices;
            if(columnName==clusterColumn){
                // only one because it is clustered by this column
                indices = new Vector<Integer>();
                indices.add(AdamHelpers.binarySearchPageAdam(page,columnName,k));
            }else{
                indices = AdamHelpers.linearSearchPageAdam(page.getTuples(),columnName,k);
            }
            if(indices.size()==0){
                // Dont know how this will happen since the index points at that page so i think index is broken
                // TODO Change this
                System.out.println(name + " Index has incorrect pointer to page");
            }else{
                for(int j=0;j<indices.size();j++){
                    result.add(tuples.get(j));
                }
            }
        }
        return result;
    }

    // public Vector<Tuple> selectNotEqualTuples(String columnName,Comparable<Key> k) throws Exception{
    //     Vector<Tuple> result = new Vector<Tuple>();
    //     Vector<String> pageNames = bTree.search(k);
    //     Table table;
    //     try {
    //         Table table = (Table) Serializer.deSerialize(tableName);
    //     } catch (Exception e) {
    //         // TODO: handle exception
    //         throw new DBAppException("The tree cannot find the table.");
    //     }
    //     Vector<String> tablePageNames = table.getPageFileNames();
    //     if(pageNames.size()==0){
    //         // No value equal to k in B+ Tree
    //         // So all tuples in all pages are added into the result vector.
    //         for(int i=0;i<tablePageNames.size();i++){
    //             Page page;
    //             try {
    //                 page = (Page) Serializer.deSerialize(tablePageNames.get(i));
    //             } catch (Exception e) {
    //                 // TODO: handle exception
    //             }
    //             result.addAll(page.getTuples());
    //         }
    //         return result;
    //     }
    //     for(int i=0;i<tablePageNames.size();i++){
    //         Page page;
    //         try {
    //             page = (Page) Serializer.deSerialize(tablePageNames.get(i));
    //         } catch (Exception e) {
    //             // TODO: handle exception
    //         }
    //         if(pageNames.contains(tablePageNames)){
    //             // This page contains the key so we have to loop to see which is it

    //         }
    //         result.addAll(page.getTuples());
    //     }
    //     return result;
    //     for(int i=0;i<pageNames.size();i++){
    //         Page page;
    //         try {
    //             page = (Page) Serializer.deSerialize(pageNames.get(i));
    //         } catch (Exception e) {
    //             // TODO: handle exception
    //             throw new Exception("Deserializer Error");
    //         }
    //         Vector<Tuple> tuples = page.getTuples();
    //         Vector<Integer> indices;
    //         if(columnName==clusterColumn){
    //             // only one because it is clustered by this column
    //             indices = new Vector<Integer>();
    //             indices.add(AdamHelpers.binarySearchPageAdam(page,columnName,k));
    //         }else{
    //             indices = AdamHelpers.linearSearchPageAdam(page.getTuples(),columnName,k);
    //         }
    //         if(indices.size()==0){
    //             // Dont know how this will happen since the index points at that page so i think index is broken
    //             // TODO Change this
    //             System.out.println(name + " Index has incorrect pointer to page");
    //         }else{
    //             for(int j=0;j<indices.size();j++){
    //                 result.add(tuples.get(j));
    //             }
    //         }
    //     }
    //     return result;
    // }

    public HashMap<Page,Vector<Integer>> selectRange(String columnName,Comparable<Key> lower,Comparable<Key> upper,boolean lowerBoundInclusive,boolean upperBoundInclusive) throws Exception{
        HashMap<Page,Vector<Integer>> result = new HashMap<Page,Vector<Integer>>();
        HashMap<Comparable,Vector<String>> pageNamesMap = bTree.search(lower,upper,lowerBoundInclusive,upperBoundInclusive);; // Results in vector of vectors of page names
        if(pageNamesMap.size()==0){
            // Btree does not contain value
            return result;
        }
        for (Map.Entry<Comparable,Vector<String>> entry : pageNamesMap.entrySet()) {
            Comparable key = entry.getKey();
			Vector<String> pageNames = pageNamesMap.get(key);
            for(int j=0;j<pageNames.size();j++){
                Page page;
                try {
                    page = (Page) Serializer.deSerialize(pageNames.get(j));
                } catch (Exception e) {
                    // TODO: handle exception
                    throw new Exception("Deserializer Error");
                }
                Vector<Integer> indices;
                if(columnName==clusterColumn){
                    // only one because it is clustered by this column
                    indices = new Vector<Integer>();
                    indices.add(AdamHelpers.binarySearchPageAdam(page,columnName,key));
                }else{
                    indices = AdamHelpers.linearSearchPageAdam(page.getTuples(),columnName,key);
                }
                // Vector<Integer> indices = AdamHelpers.linearSearchPageAdam(page.getTuples(),columnName,lower);
                if(indices.size()==0){
                    // Dont know how this will happen since the index points at that page so i think index is broken
                    // TODO Change this
                    System.out.println(name + " Index has incorrect pointer to page");
                }else{
                    result.put(page, indices); // Add index to array list for that page
                }
            }
		}
        return result;
    }

    public Vector<Tuple> selectRangeTuples(String columnName,Comparable<Key> lower,Comparable<Key> upper,boolean lowerBoundInclusive,boolean upperBoundInclusive) throws Exception{
        Vector<Tuple> result = new Vector<Tuple>();
        HashMap<Comparable,Vector<String>> pageNamesMap = bTree.search(lower,upper,lowerBoundInclusive,upperBoundInclusive); // Results in vector of vectors of page names
        if(pageNamesMap.size()==0){
            // Btree does not contain value
            return result;
        }
        for (Map.Entry<Comparable,Vector<String>> entry : pageNamesMap.entrySet()) {
            Comparable key = entry.getKey();
			Vector<String> pageNames = pageNamesMap.get(key);
            for(int i=0;i<pageNames.size();i++){
                Page page;
                try {
                    page = (Page) Serializer.deSerialize(pageNames.get(i));
                } catch (Exception e) {
                    // TODO: handle exception
                    throw new Exception("Deserializer Error");
                }
                Vector<Tuple> tuples = page.getTuples();
                Vector<Integer> indices;
                if(columnName==clusterColumn){
                    // only one because it is clustered by this column
                    indices = new Vector<Integer>();
                    indices.add(AdamHelpers.binarySearchPageAdam(page,columnName,key));
                }else{
                    indices = AdamHelpers.linearSearchPageAdam(page.getTuples(),columnName,key);
                }
                if(indices.size()==0){
                    // Dont know how this will happen since the index points at that page so i think index is broken
                    // TODO Change this
                    System.out.println(name + " Index has incorrect pointer to page");
                }else{
                    for(int j=0;j<indices.size();j++){
                        result.add(tuples.get(j));
                    }
                }
            }
		}
        return result;
    }

    public Hashtable<String,Vector<Integer>> selectEqualNGMSAA(String columnName,Comparable<Key> k) throws Exception{
        Hashtable<String,Vector<Integer>> result = new Hashtable<String,Vector<Integer>>();
        Vector<String> pageNames = bTree.search(k);
        if(pageNames.size()==0){
            // Btree does not contain value
            return result;
        }
        for(int i=0;i<pageNames.size();i++){
            Page page;
            try {
                page = (Page) Serializer.deSerialize(pageNames.get(i));
            } catch (Exception e) {
                throw new Exception("Deserializer Error");
            }
            
            Vector<Integer> indices = AdamHelpers.linearSearchPageAdam(page.getTuples(),columnName,k);
            if(indices.size()==0){
                System.out.println(name + " Index has incorrect pointer to page");
            }else{
                result.put(page.fileName, indices);
            }
        }
        return result;
    }

    public static void main(String[] args) {
        // Hashtable htblColNameType = new Hashtable( );
        // htblColNameType.put("id", "java.lang.Integer");
        // htblColNameType.put("name", "java.lang.String");
        // htblColNameType.put("gpa", "java.lang.double");
        // Table table = new Table("Players", "id",htblColNameType );
        // Page page = new Page(null);
    }
}
