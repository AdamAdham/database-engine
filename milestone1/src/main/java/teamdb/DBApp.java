package teamdb;


/** * @author Wael Abouelsaadat */ 

import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serial;
//import java.util.Vector;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map.Entry;
import teamdb.classes.*;
import teamdb.classes.bplustree.LeafNode;

public class DBApp {

	private Vector<String> tableNamesArray;
	public File csv;

	public DBApp( ){
		init();
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){
		tableNamesArray = new Vector<String>();
	}


	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data 
	// type as value
	public void createTable(String strTableName, 
							String strClusteringKeyColumn,  
							Hashtable<String,String> htblColNameType) throws DBAppException{
		if(Serializer.wasSerialized(strTableName)){
			throw new DBAppException("Table already exists");
		}		
		Table table = new Table(strTableName,strClusteringKeyColumn,htblColNameType);
		tableNamesArray.add(strTableName);
		try {
			updateMeta();
		} catch (Exception e) {
		}
	}


	// following method creates a B+tree index 
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException{
		
		if(!Serializer.wasSerialized(strTableName)){
			throw new DBAppException("Table does not exist");
		}
		if(Serializer.wasSerialized(strIndexName)){
			throw new DBAppException("Index already exists");
		}	
		Table table;
		try {
			table = (Table) Serializer.deSerialize(strTableName);
		} catch (Exception e) {
			// TODO: handle exception
			return;
		}		
		Index index = new Index(strTableName, strColName, strIndexName,table.getClusteringKeyColumn());
		table.addIndexHash(strColName, index);
		try {
			updateMeta();
		} catch (Exception e) {
			System.out.println("Error while creating metadata");
		}
	}


	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, 
                                Hashtable<String,Object>  htblColNameValue) throws DBAppException{
								Table t;
								if(!Serializer.wasSerialized(strTableName)){
									throw new DBAppException("Table does not exist");
								}
								try {
									t = (Table) Serializer.deSerialize(strTableName);
								} catch (Exception e) {
									// TODO: handle exception
									return;
								}



                                Hashtable<String,Index> hash = t.getIndexHash();  //column name , Index
								Vector<Vector<String>> meta = getCSV();
								
								if(htblColNameValue.size()!=t.getColNameType().size()){
									// Or add the values but as null
									throw new DBAppException("Number of values are not the same");
								}
								for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
									String columnName = entry.getKey(); // Getting Key (column name)
									Object columnValue = entry.getValue();
									Class<?> columnClass = columnValue.getClass();
									String columnClassName = columnClass.getSimpleName();
									Boolean found = false; //Check if given column exists in the columns in the table

									for(int i=0;i<meta.size();i++){
										String tableNameMeta = meta.get(i).get(0);
										String columnNameMeta = meta.get(i).get(1);
										if(tableNameMeta==strTableName&&columnNameMeta==columnName){
											// At current Table and current column
											found = true;
											String columnTypeMeta = meta.get(i).get(2);

											// Checking that the type of input and type of column in metadata are the same, if not throw an exception
											if(columnTypeMeta=="java.lang.Integer"&&columnClassName!="int"){
												// Debug in seperate file
												throw new DBAppException("Incompatible Data Type");
											}else
											if(columnTypeMeta=="java.lang.String"&&columnClassName!="String"){
												// Debug in seperate file
												throw new DBAppException("Incompatible Data Type");
											}else
											if(columnTypeMeta=="java.lang.Double"&&columnClassName!="double"){
												// Debug in seperate file
												throw new DBAppException("Incompatible Data Type");
											}	
										}								
									}
									// If said column not in metadata then throw exception
									if(!found){
										throw new DBAppException(columnName + " does not exist in the table");
									}
								}							
								
								Index index;

								// Stopped Here

								//continued --> tatos

								// Transfer contents from Hashtable to HashMap

								HashMap<String, Object> hashMap1 = new HashMap<>();
								for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
									hashMap1.put(entry.getKey(), entry.getValue());
								}

								Tuple tupletoinsert = new Tuple(hashMap1);	// our tuple with values to insert

						String primarykeyy = t.getClusteringKeyColumn();
                         String thedesiredpagename = "";
									//table has an index on the primary key column --> we'll use it in the insertion
						 if(!hash.containsKey(primarykeyy)){
                               // if(hash.size() == 0) {
									//no index to use in the insertion 


                                    Object [] binarySearchReturn = binarysearchkonato(t, tupletoinsert);

						// 3 return cases in binarysearchkonato: 
						//  1) string: this will be the first tuple to insert
						//  2) contains -1 : my tuple's PK is the smallest PK 
						//  3) normal ({rakameltuple, lastpage1, lastpage, rakamelpage})
		                            Page firstPage;
									if (binarySearchReturn[0] instanceof String) {
										try{
											 firstPage = t.createPage();
											
										}
										catch (Exception ex){
                                              throw new DBAppException("hasal moshkela f creating awel page khales");
											  
										}
                                         try{
											Page despage = (Page) Serializer.deSerialize(firstPage.fileName);
											thedesiredpagename = despage.fileName;
											String ourinsertedrow = despage.addTuple(tupletoinsert);  //return string won't be used
											//serialize the page
											Serializer.serialize(despage, despage.fileName);
										 }
										 catch(Exception ex){
											// handle the exception here
										 }
										

									} else if (binarySearchReturn[0] instanceof Integer) {
										if((int)binarySearchReturn[0] == -1){

										thedesiredpagename = insertinpageskonato(t, tupletoinsert,0, -1);
										}
										else{
											int rakameltuple = (int) binarySearchReturn[0];  //tuple just before the tuple we wish to insert
											int rakamelpage = (int) binarySearchReturn[3]; 
										thedesiredpagename = insertinpageskonato(t, tupletoinsert, rakamelpage, rakameltuple);
										}

									} 

								}
								// to be continued

								//recall: Hashtable<String,Index> hash = t.getIndexHash();  //column name , Index
								else{
									


									String primarykey = t.getClusteringKeyColumn();

									//table has an index on the primary key column --> we'll use it in the insertion
									   Index clusteringindex = hash.get(primarykey);
									   bplustree bp = clusteringindex.getBTree();
									   Object VTpInBt = tupletoinsert.getValue(primarykey);
									   Comparable comp = (Comparable) VTpInBt;
                                        thedesiredpagename = bplustree.helperforindexsearch(bp,comp);
									   int rakamofthepage = 1;
									   int i = 0;
									   while(t.getPageFileNames().get(i)!= thedesiredpagename){
                                            i = i + 1;
                                            rakamofthepage = rakamofthepage +1;
									   }
									   Page thedesiredpage;
									   
									   try{
										 thedesiredpage = (Page) Serializer.deSerialize(thedesiredpagename);
										 Serializer.serialize(thedesiredpage, thedesiredpage.fileName);
										 int rakamofthetuple = binarysearchkonato2(thedesiredpage, tupletoinsert, t);
                                        String notimportantstring =  insertinpageskonato(t,tupletoinsert,rakamofthepage,rakamofthetuple);
									   }
									   catch(Exception ex){
										// handle the exception here
									   }
									  
									
									   // Get the enumeration of keys --> enumeration object containing all keys in the hashtable
												// all column names for columns that have an index
												/*Enumeration<String> keys = hash.keys();
												boolean flagx = true;
												while(flagx){
													if (keys.hasMoreElements()) {
														// Get the first key  (column name)
														String firstKey = keys.nextElement();
														
													
														// Retrieve the value associated with the first key
														Index firstValue = hash.get(firstKey);

														bplustree treex = firstValue.getBTree();
														// treex.insert(firstKey, firstValue); TODO firstvalue is add as index?
														flagx = true;
													
														
													} else {
														flagx = false;
														//inserted into all indexes we have
													}
												}*/

									
									   
								   
								
								}

								for (Map.Entry<String, Index> entry : hash.entrySet()) {
									String columnName = entry.getKey(); // Getting Key (column name)
									index = entry.getValue();
									bplustree tree = index.getBTree();
									Comparable o1;


									//checking that the column that you have it's index is the same as the column you'll insert into
									for (Map.Entry<String, Object> entry1 : htblColNameValue.entrySet()) {
										String column = entry1.getKey(); // Getting Key (column name)
										o1 = (Comparable)entry1.getValue();
										if(column.equals(columnName)){
											tree.insert(o1, thedesiredpagename);
											break;
										}
									}

								}
                                
                            
                                
      //  throw new DBAppException("not implemented yet");
    }


	public void updateTable(String strTableName, 
            String strClusteringKeyValue,
            Hashtable<String,Object> htblColNameValue)  throws Exception{
		//should be changed according to what we will understand from index

			Table table = (Table) Serializer.deSerialize(strTableName);
			String clusterKey = table.getClusteringKeyColumn();
			Hashtable<String,Vector<Integer>> searchResult = binarySearch(strTableName,clusterKey,strClusteringKeyValue);

			//Hashtable<String,Vector<Integer>> searchResult = search(strTableName,searcher);
			System.out.println("SearchResult: " + searchResult);
	        for (Map.Entry<String, Vector<Integer>> entry : searchResult.entrySet()) {
        		Page p = (Page) Serializer.deSerialize(entry.getKey());
	        	for(int i = 0; i < entry.getValue().size(); i++) {
	        		Tuple newTuple = (p.getTuples().get(entry.getValue().get(i))).updateTuple(htblColNameValue);
	        		p.setTuple(entry.getValue().get(i), newTuple);
	        	}
        		Serializer.serialize(p, entry.getKey());
	        }
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws Exception{
		//should be changed according to what we will understand from index
		Table table = (Table) Serializer.deSerialize(strTableName);
		Hashtable<String,Vector<Integer>> searchResult = search(strTableName,htblColNameValue);
		for (Map.Entry<String, Vector<Integer>> entry : searchResult.entrySet()) {
			Page page = (Page) Serializer.deSerialize(entry.getKey());
			Vector<Tuple> newTuples = page.getTuples();
			for(int i = 0; i < entry.getValue().size(); i++) {
				newTuples.remove((int)entry.getValue().get(i));
			}
			if (page.isEmpty()) {
				table.removemtpage(page);
			}
			else {
				page.setTuples(newTuples);
			Serializer.serialize(page, page.fileName);
			}
		}
	}

    @SuppressWarnings("unchecked")
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[]  strarrOperators) throws DBAppException{
        Vector<Tuple> list = new Vector<>();
		for(int i=0;i<arrSQLTerms.length;i++) {
			Vector<Tuple> tempList = new Vector<>();
			String tableName = arrSQLTerms[i]._strTableName;
			String columnName = arrSQLTerms[i]._strColumnName;
			String operator = arrSQLTerms[i]._strOperator;
			Comparable value = (Comparable) arrSQLTerms[i]._objValue;
			if(!Serializer.wasSerialized(tableName)){
				throw new DBAppException("Table does not exist.");
			}
			Table table;
			try {
				table = (Table) Serializer.deSerialize(tableName);
			} catch (Exception e) {
				// TODO: handle exception
				throw new DBAppException("Error while fetching table.");		
			}
			if(!table.getColNameType().containsKey(columnName)){
				throw new DBAppException("Column: " + columnName + " is not in table.");		
			}
			Hashtable indexHash = table.getIndexHash();
			Index index = (Index) indexHash.get(columnName);

			if(operator=="!="){
				// Loop through all pages and just add the ones that don't have this element
				try {
					Vector<String> tablePageNames = table.getPageFileNames();
					for(int j=0;j<tablePageNames.size();j++){
						Page page;
						try {
							page = (Page) Serializer.deSerialize(tablePageNames.get(j));
						} catch (Exception e) {
							// TODO: handle exception
							throw new DBAppException("Deserializer did not work: Page names are incorrect at table: "+ tableName);
						}
						Vector<Tuple> tuples = page.getTuples();
						for(int k=0;k<tuples.size();k++){
							Tuple tuple = tuples.elementAt(k);
							HashMap<String,Object> values = tuple.getValues();
							Comparable val = (Comparable) values.get(columnName);
							if(val.compareTo(value) != 0){
								tempList.add(tuple);
							}
						}       
					}
				} catch (Exception e) {
					// TODO: handle exception
					throw new DBAppException("Deserializer did not work");
				}

			}else{
				// Not != operator

				if(index!=null){
					// Column is indexed
					switch (operator) {
						case "=":
						//Type safety: The expression of type HashMap needs unchecked conversion to conform to HashMap<Page,Vector<Integer>>, Solved by:@SuppressWarnings("unchecked")
						try {
							// hena technically i can just return an array of tuples
							tempList = index.selectEqualTuples(columnName,value);
						} catch (Exception e) {
							// TODO: handle exception
							throw new DBAppException("Selecting from index did not work");
						}
						break;
	
						case ">":
							try {
								tempList = index.selectRangeTuples(columnName, value, null, false, false);
							} catch (Exception e) {
								// TODO: handle exception
								throw new DBAppException("Selecting from index did not work");
							}
							
							break;				
					
						case ">=":
							try {
								tempList = index.selectRangeTuples(columnName, value, null, true, false);
							} catch (Exception e) {
								// TODO: handle exception
								throw new DBAppException("Selecting from index did not work");
							}
						
							break;
	
						case "<":
							try {
								tempList = index.selectRangeTuples(columnName, null, value, false, false);
							} catch (Exception e) {
								// TODO: handle exception
								throw new DBAppException("Selecting from index did not work");
							}
							break;
	
						case "<=":
							try {
								tempList = index.selectRangeTuples(columnName, null, value, false, true);
							} catch (Exception e) {
								// TODO: handle exception
								throw new DBAppException("Selecting from index did not work");
							}
							break;
	
						default:
							throw new DBAppException("Operator: " + operator + " is not supported");
					}
				}else{
					if(columnName.equals(table.getClusteringKeyColumn())){
						switch (operator) {
							case "=":
								try {
									// hena technically i can just return an array of tuples
									tempList.add(AdamHelpers.binarySearchAdam(table, value));
								} catch (Exception e) {
									// TODO: handle exception
									throw new DBAppException("Selecting from pages did not work");
								}
								break;
		
							case ">":
								try {
									tempList = index.selectRangeTuples(columnName, value, null, false, false);
								} catch (Exception e) {
									// TODO: handle exception
									throw new DBAppException("Selecting from index did not work");
								}
								
								break;				
						
							case ">=":
								try {
									tempList = index.selectRangeTuples(columnName, value, null, true, false);
								} catch (Exception e) {
									// TODO: handle exception
									throw new DBAppException("Selecting from index did not work");
								}
							
								break;
		
							case "<":
								try {
									tempList = index.selectRangeTuples(columnName, null, value, false, false);
								} catch (Exception e) {
									// TODO: handle exception
									throw new DBAppException("Selecting from index did not work");
								}
								break;
		
							case "<=":
								try {
									tempList = index.selectRangeTuples(columnName, null, value, false, true);
								} catch (Exception e) {
									// TODO: handle exception
									throw new DBAppException("Selecting from index did not work");
								}
								break;
		
							default:
								throw new DBAppException("Operator: " + operator + " is not supported");
						}
					}
				}
			}			
			
		}
										
		return list.iterator(); // TODO Not sure
	}


	public static void main( String[] args ) throws Exception{
	
	try{
			String strTableName = "Student";
			DBApp	dbApp = new DBApp( );
			
			Hashtable htblColNameType = new Hashtable( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.double");
			dbApp.createTable( strTableName, "id", htblColNameType );
			dbApp.createIndex( strTableName, "gpa", "gpaIndex" );

			// Hashtable htblColNameValue = new Hashtable( );
			// htblColNameValue.put("id", new Integer( 2343432 ));
			// htblColNameValue.put("name", new String("Ahmed Noor" ) );
			// htblColNameValue.put("gpa", new Double( 0.95 ) );
			// dbApp.insertIntoTable( strTableName , htblColNameValue );

			// htblColNameValue.clear( );
			// htblColNameValue.put("id", new Integer( 453455 ));
			// htblColNameValue.put("name", new String("Ahmed Noor" ) );
			// htblColNameValue.put("gpa", new Double( 0.95 ) );
			// dbApp.insertIntoTable( strTableName , htblColNameValue );

			// htblColNameValue.clear( );
			// htblColNameValue.put("id", new Integer( 5674567 ));
			// htblColNameValue.put("name", new String("Dalia Noor" ) );
			// htblColNameValue.put("gpa", new Double( 1.25 ) );
			// dbApp.insertIntoTable( strTableName , htblColNameValue );

			// htblColNameValue.clear( );
			// htblColNameValue.put("id", new Integer( 23498 ));
			// htblColNameValue.put("name", new String("John Noor" ) );
			// htblColNameValue.put("gpa", new Double( 1.5 ) );
			// dbApp.insertIntoTable( strTableName , htblColNameValue );

			// htblColNameValue.clear( );
			// htblColNameValue.put("id", new Integer( 78452 ));
			// htblColNameValue.put("name", new String("Zaky Noor" ) );
			// htblColNameValue.put("gpa", new Double( 0.88 ) );
			// dbApp.insertIntoTable( strTableName , htblColNameValue );


			// SQLTerm[] arrSQLTerms;
			// arrSQLTerms = new SQLTerm[2];
			// arrSQLTerms[0]._strTableName =  "Student";
			// arrSQLTerms[0]._strColumnName=  "name";
			// arrSQLTerms[0]._strOperator  =  "=";
			// arrSQLTerms[0]._objValue     =  "John Noor";

			// arrSQLTerms[1]._strTableName =  "Student";
			// arrSQLTerms[1]._strColumnName=  "gpa";
			// arrSQLTerms[1]._strOperator  =  "=";
			// arrSQLTerms[1]._objValue     =  new Double( 1.5 );

			// String[]strarrOperators = new String[1];
			// strarrOperators[0] = "OR";
			// // select * from Student where name = "John Noor" or gpa = 1.5;
			// Iterator<Tuple> resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
		// DBApp db = new DBApp();
    	// Hashtable<String, String> ht = new Hashtable<String, String>();
    	// ht.put("id", "java.lang.Integer");
    	// ht.put("name", "java.lang.String");
    	// ht.put("gpa", "java.lang.Double");
    	// db.createTable("Student", "id", ht);
		// Table table = (Table) Serializer.deSerialize("Student");
		// table.setClusterKey("id");
		// Serializer.serialize(table, table.getTableName());

    	// Page page = table.createPage();
    	// Page p2 = table.createPage();
    	// Page p3 = table.createPage();
    	// Page p4 = table.createPage();
    	// HashMap<String, Object> hm1 = new HashMap<String, Object>();
    	// HashMap<String, Object> hm2 = new HashMap<String, Object>();
    	// HashMap<String, Object> hm3 = new HashMap<String, Object>();
    	// HashMap<String, Object> hm4 = new HashMap<String, Object>();
    	// HashMap<String, Object> hm5 = new HashMap<String, Object>();

    	// hm1.put("id", "1221");
    	// hm1.put("name", "ahmed");
    	// hm1.put("gpa", 3.5);
    	// Tuple t1 = new Tuple(hm1);
    	// page.addTuple(t1);

    	
    	// hm2.put("id", "1222");
    	// hm2.put("name", "ahmed");
    	// hm2.put("gpa", 3.2);
    	// Tuple t2 = new Tuple(hm2);
    	// p2.addTuple(t2);
    	
    	// hm3.put("id", "2076");
    	// hm3.put("name", "ali");
    	// hm3.put("gpa", 3.1);
    	// Tuple t3 = new Tuple(hm3);
    	// p3.addTuple(t3);
    	
    	// hm4.put("id", "3055");
    	// hm4.put("name", "adam");
    	// hm4.put("gpa", 2);
    	// Tuple t4 = new Tuple(hm4);
    	// p4.addTuple(t4);
    	
    	// hm5.put("id", "7000");
    	// hm5.put("name", "engy");
    	// hm5.put("gpa", 0);
    	// Tuple t5 = new Tuple(hm5);
    	// p4.addTuple(t5);
    	
    	// Serializer.serialize(page, "Student1");
    	// Serializer.serialize(p2, "Student2");
    	// Serializer.serialize(p3, "Student3");
    	// Serializer.serialize(p4, "Student4");
    	
    	// Serializer.serialize(table,table.getTableName());

    	
    	// Hashtable<String, Object> hts = new Hashtable<String, Object>();
    	// hts.put("name", "shiko");
    	// //hts.put("id", "1221");
    	// //System.out.println("");
    	// //System.out.println(p4);
    	// //db.updateTable("Student", "engy", hts);
    	// //Page pNew = (Page) Serializer.deSerialize("Student4");
    	// //System.out.println("blabla");
    	// System.out.println(Serializer.deSerialize("Student"));
    	// //db.deleteFromTable("Student", hts);
    	// db.updateTable("Student", "7000", hts);
    	// System.out.println("");
    	// System.out.println(Serializer.deSerialize("Student")); 
	}

	public void updateMeta() throws FileNotFoundException {
		if(csv == null) {
			csv = new File("metadata.csv");
			System.out.print("something");
		}
		PrintWriter out = new PrintWriter(csv);
		
		out.printf("%s,%s,%s,%s,%s,%s\n","Table Name", "Column Name", "Column Type", "ClusteringKey", "IndexName","IndexType");
		for(int i=0;i<tableNamesArray.size();i++) {
			String tableName = tableNamesArray.get(i);
			Table table;
			try {
				table = (Table) Serializer.deSerialize(tableName);
			} catch (Exception e) {
				// TODO: handle exception	
				return;			
			}			
			Hashtable<String,String> ht = table.getColNameType();
			for (Map.Entry<String, String> entry : ht.entrySet()) {
				String hkey = entry.getKey();
				String value = entry.getValue();
				Index index = table.getIndexHash().get(hkey);
				String indexName = "Null";
				String indexType = "null";
				if(index!=null){
					indexName = index.getName();
					indexType = "B+tree";
				}
				String clusterBool = "False";
				if (hkey.equals(table.getClusteringKeyColumn())) {
					clusterBool = "True";
				}
				out.printf("%s,%s,%s,%s,%s,%s\n",table.getTableName(), hkey, value, clusterBool, indexName, indexType);
			}
		}	
		out.close();
	}

	public Vector<Vector<String>> getCSV() {

        String csvFile = "metadata.csv"; // Path to your CSV file
        String csvDelimiter = ","; // CSV delimiter (usually comma)

        Vector<Vector<String>> data = new Vector<>();

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(csvDelimiter);
                Vector<String> row = new Vector<>();
                for (String value : values) {
                    row.add(value);
                }
                data.add(row);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Print the 2D Vector
        for (Vector<String> row : data) {
            for (String value : row) {
                System.out.print(value + "\t");
            }
            System.out.println();
        }
        return data;
    }

	public Hashtable<String,Vector<Integer>> search(String strTableName, Hashtable<String,Object> ht) throws Exception {
		
		Hashtable<String,Vector<Integer>> ret = null;

		
		Table table = (Table) Serializer.deSerialize(strTableName);
		String clusteringKey = table.getClusteringKeyColumn();
		Object clusteringKeyValue = null;
		
		for (Entry<String, Object> entry : ht.entrySet()) {
			if(entry.getKey().equals(clusteringKey)) {
				clusteringKeyValue = entry.getValue();
			}
			//break;
		}
		ret = searchByIndex(strTableName, ht);
		if((ret != null) && (searchByIndex(strTableName, ht).size() > 0)) {
			System.out.println("Performing index search:");
		} else {
			if(clusteringKeyValue != null) {
				System.out.println("Performing binary search:");
				ret = binarySearch(strTableName,clusteringKey,clusteringKeyValue);
			} else {
				System.out.println("Performing linear search:");
				ret = linearSearch(strTableName,ht);
			}
		}
		
		return ret;
	}

	public Hashtable<String,Vector<Integer>> searchByIndex(String strTableName, Hashtable<String,Object> ht) throws Exception {
		Hashtable<String,Vector<Integer>> ret = null;
		Table table = (Table) Serializer.deSerialize(strTableName);
		boolean flag = false;
		for (Entry<String, Object> entry : ht.entrySet()) {
			String indexName = hasIndex(getCSV(), strTableName, entry.getKey());
			if(indexName != null) {
				Index index = table.getIndexHash().get(indexName);
				try {
					Comparable tempValue = (Comparable) entry.getValue();
					if(!flag) {
						ret = index.selectEqualNGMSAA(entry.getKey(),tempValue);
						flag = true;
					} else {
						Hashtable<String,Vector<Integer>> temp = index.selectEqualNGMSAA(entry.getKey(),tempValue);
						Hashtable<String,Vector<Integer>> intersection = new Hashtable<String,Vector<Integer>>();
						for (Entry<String, Vector<Integer>> innerEntry : temp.entrySet()) {
							if(ret.containsKey(innerEntry.getKey()) && ret.get(innerEntry.getKey()).equals(innerEntry.getValue())) {
								intersection.put(innerEntry.getKey(), innerEntry.getValue());
							}
						}
						ret = intersection;
					}
				} catch (Exception e) {
					throw new DBAppException("Deserializer did not work");
				}
			} else {
				return null;
			}
		}
		return ret;
	}

    public Hashtable<String,Vector<Integer>> linearSearch(String strTableName, Hashtable<String,Object> ht) throws Exception {
		Hashtable<String,Vector<Integer>> arr = new Hashtable<String,Vector<Integer>>();
		Table table = (Table) Serializer.deSerialize(strTableName);
		Vector<String> pageFileNames = table.getPageFileNames();
		for(int i = 0; i < pageFileNames.size(); i++) {
			Page p = (Page) Serializer.deSerialize(pageFileNames.get(i));
			Vector<Integer> temp = new Vector<Integer>();
			for(int j = 0; j < p.getTuples().size(); j++) {
				Tuple tuple = p.getTuples().get(j);
				HashMap<String, Object> values = tuple.getValues();
				boolean flag = true;
				for (Entry<String, Object> entry : ht.entrySet()) {
					if(!entry.getValue().equals(values.get(entry.getKey()))) {
						flag = false;
						break;
					}
				}
				if(flag) {
					temp.add(j);
				}
				
			}
			if(temp.size() != 0) {
				arr.put(p.fileName, temp);
			}
		}
		System.out.print("Found: \n" + arr);
		return arr;
	}
	
	public Hashtable<String,Vector<Integer>> binarySearch(String strTableName, String key, Object value) throws Exception { //used only when searching with clustering key
		Hashtable<String,Vector<Integer>> result = new Hashtable<String,Vector<Integer>>();
		Table table = (Table) Serializer.deSerialize(strTableName);
		Vector<String> pages = table.getPageFileNames();
		binarySearchHelper(pages, key, value, 0, (pages.size() - 1), result);
		return result;
	}
	
	@SuppressWarnings("unchecked")
	public void binarySearchHelper(Vector<String> pages, String key, Object value,int left, int right, Hashtable<String,Vector<Integer>> result) throws Exception {
		if (left > right) {
			return;
		}
		int mid = left + (right - left) / 2;
		Page p = (Page) Serializer.deSerialize(pages.get(mid));
		System.out.println("Searched through: " + p);
		Vector<Integer> indices = binarySearchInternal(p.getTuples(), key, value);
		Comparable<Object> firstTupleValue = (Comparable<Object>) p.getTuples().get(0).getValue(key);
        Comparable<Object> lastTupleValue = (Comparable<Object>) p.getTuples().get(p.getTuples().size() - 1).getValue(key);
        Comparable<Object> myValue = (Comparable<Object>) value;
        if(indices.size() != 0) {
    		result.put(p.fileName, indices);
        } else {
        	if(firstTupleValue.compareTo(myValue) == 0) {
                binarySearchHelper(pages, key, value, left, mid - 1, result);
    		} else if (lastTupleValue.compareTo(myValue) == 0){
                binarySearchHelper(pages, key, value, mid + 1, right, result);
    		}
            if(firstTupleValue.compareTo(myValue) > 0) {
                binarySearchHelper(pages, key, value, left, mid - 1, result);
            } else if(lastTupleValue.compareTo(myValue) < 0) {
                binarySearchHelper(pages, key, value, mid + 1, right, result);
            } else {
            	return;
            }
        }
	}
	
	public Vector<Integer> binarySearchInternal(Vector<Tuple> tuples, String key, Object value) {
		Vector<Integer> result = new Vector<Integer>();
        binarySearchRecursiveInternal(tuples, key, value, 0, tuples.size() - 1, result);
        return result;
    }

	private void binarySearchRecursiveInternal(Vector<Tuple> tuples, String key, Object value, int left, int right, Vector<Integer> result) {
        if (left > right) {
            return;
        }
        
        int mid = left + (right - left) / 2;
        Tuple tuple = tuples.get(mid);
        String tupleValue = (tuple.getValues().get(key) + "");
        
        int comparator = tupleValue.compareTo((String) value);
        
        if (comparator == 0) {
        	result.add(mid);
            binarySearchRecursiveInternal(tuples, key, value, left, mid - 1, result);
            binarySearchRecursiveInternal(tuples, key, value, mid + 1, right, result);
        } else if (comparator < 0) {
            binarySearchRecursiveInternal(tuples, key, value, mid + 1, right, result);
        } else {
            binarySearchRecursiveInternal(tuples, key, value, left, mid - 1, result);
        }
    }

	public String hasIndex(Vector<Vector<String>> csv, String strTableName, String strColName) {
		for(int i = 0; i < csv.size();i++) {
			if(csv.get(i).get(0).equals(strTableName) && csv.get(i).get(1).equals(strColName)) {
				return csv.get(i).get(4);
			}
		}
		return "Null";
	}
	
	

	public static String insertinpageskonato(Table table, Tuple tuple, int rakamelpage, int rakameltuple){
		int theactualrakamofpage = rakamelpage;
		Vector<String> pageFileNames = table.getPageFileNames();
        
               boolean flag = true;
				String page = pageFileNames.get(rakamelpage);
				Page page1;
				try {
					
					page1 = (Page) Serializer.deSerialize(page);
				
				} catch (Exception e) {
			
					return "";
				}
				Vector<Tuple> tuples = page1.getTuples();
				int newindex = rakameltuple +1;  //position I will insert into
				if((newindex == tuples.size()) && (tuples.size() ==  (page1.getMaxRows()+1)) ){

                  theactualrakamofpage = theactualrakamofpage +1;
				} 
                while (flag){
                    
					tuples.add(newindex, tuple);
					if(tuples.size() == (page1.getMaxRows()+1)){
						if(rakamelpage == (pageFileNames.size()-1)){  //check if it's the last page
							try{
								Page newest = table.createPage();
								String esmelPage = newest.getFileName();
								tuple = tuples.elementAt(tuples.size()-1); 
								tuples.remove(tuples.size()-1);
							
                                Serializer.serialize(page1,page1.fileName);
								tuples = newest.getTuples();
								 //removing the extra tuple from our full page (not the new created one)
								tuples.add(tuple);

								//Serializer.serialize(newest, esmelPage); //check law el page beyet3emelaha serialize upon creation or not (serialize twice 3ady wala la2)
								//Serializer.serialize(page1,page);
								break;
							}
							catch(Exception ex){

							}
							
						}
						tuple = tuples.elementAt(tuples.size()-1);  
						tuples.remove(tuples.size()-1);

					
				

						rakamelpage = rakamelpage +1;  //next page number
						String nextpage = pageFileNames.get(rakamelpage); // next page name
						//Page nextpage1;

						
						try {
						//	Serializer.serialize(page1,page); // testing --> serialize the first deserialized page (won't be used)
						    Serializer.serialize(page1, page1.fileName);
							page1 = (Page) Serializer.deSerialize(nextpage); // page1 is now holding the next page in the deserialized form
						
						} catch (Exception e) {
					       // handle the exception here
							return "";
						}
						 tuples = page1.getTuples();
						 newindex = 0;
					}
					else{
						flag = false;
						try{
							Serializer.serialize(page1, page1.fileName);
						}
						catch(Exception ex){
							// handle the exception here
						}
						break;
					}
				}
				String thedesiredpage = table.getPageFileNames().get(theactualrakamofpage);
				return thedesiredpage;
			   
		  
	}




	public static Object [] binarysearchkonato(Table table, Tuple tuple){
		
		String clusterColumnName = table.getClusteringKeyColumn();
		Vector<String> pageFileNames = table.getPageFileNames();
		int i =0;
		int j=pageFileNames.size()-1;
		boolean flag = true;
	    if(j == -1){
            Object [] theoutput = {"this will be the first tuple to insert"};
			// indicating that we will need to create the first page for the table and insert our first tuple
		    return theoutput; 
		}

		Page page1;
		Page firstpage1;
		Page lastpage1;
        String firstpage = pageFileNames.get(0);
		String lastpage = pageFileNames.get(j);
		try {
			firstpage1 = (Page) Serializer.deSerialize(firstpage);
			Serializer.serialize(firstpage1, firstpage1.fileName); // we need to check on that concept of serializing when we are sure that we won't change the page content
		    
		} catch (Exception e) {
			Object [] pageandloc = {-1,-1};
			// TODO: handle exception
			return pageandloc ;
		}
		Vector<Tuple> tuplesfirstp = firstpage1.getTuples();
		Tuple minforfp = tuplesfirstp.elementAt(0);
         if(tuple.compareTo(minforfp, clusterColumnName)<0){
			// indicating that the tuple we want to insert will be the tuple with the smallest primary key
			// first object is location of tuple that will be just before the tuple we wish to insert
			Object [] theoutput = {-1, firstpage1, firstpage};
		    return theoutput;
		 }


		 try {
			lastpage1 = (Page) Serializer.deSerialize(lastpage);
			Serializer.serialize(lastpage1, lastpage1.fileName);
		
		} catch (Exception e) {
			Object [] pageandloc = {-5,-1};
			// TODO: handle exception
			return pageandloc ;
		}
        
		Vector<Tuple> tupleslastp = lastpage1.getTuples();
		Tuple minforlp = tupleslastp.elementAt(0);
         if(tuple.compareTo(minforlp, clusterColumnName)>0){
			int rakameltuple = binarysearchkonato2(lastpage1, tuple, table);
			// the tuple we want to insert will be located in the last page if there is space
			// first object is location of tuple that will be just before the tuple we wish to insert
			Object [] theoutput = {rakameltuple, lastpage1, lastpage, (pageFileNames.size()-1)};
		    return theoutput;
		 }
         


		while (flag){
			
			int midOfPages = (i+j)/2;
			String pageName1 = pageFileNames.get(midOfPages);
			if(midOfPages ==pageFileNames.size()-1){
				try {
					page1 = (Page) Serializer.deSerialize(pageName1);
					Serializer.serialize(page1, page1.fileName);
					int rakameltuple = binarysearchkonato2(page1, tuple, table);
				   Object [] heyy = {rakameltuple, page1, pageName1, midOfPages};
				 return heyy;
				
				} catch (Exception e) {
					 Object [] pageandloc = {-1,-1};
					// TODO: handle exception
					return pageandloc ;
				}

			}
			int oneaftermid = midOfPages +1;
			//String pageName1 = pageFileNames.get(midOfPages);
			String pageName2 = pageFileNames.get(oneaftermid);
		
			Page page2;
			try {
				page1 = (Page) Serializer.deSerialize(pageName1);
				page2 = (Page) Serializer.deSerialize(pageName2);
				Serializer.serialize(page1, page1.fileName);
				Serializer.serialize(page2, page2.fileName);
			} catch (Exception e) {
				 Object [] pageandloc = {-1,-1};
				// TODO: handle exception
				return pageandloc ;
			}
			Vector<Tuple> tuples1 = page1.getTuples();
			Vector<Tuple> tuples2 = page2.getTuples();
			int a=0;
			Tuple min1 = tuples1.elementAt(a);
			Tuple min2 = tuples2.elementAt(a);
			
			if(tuple.compareTo(min1, clusterColumnName)>0 && tuple.compareTo(min2, clusterColumnName)<0  ){
				 int rakameltuple = binarysearchkonato2(page1, tuple, table);
				 Object [] theoutput = {rakameltuple, page1, pageName1, midOfPages};
				 return theoutput;
                 // it is the page we want and i mean the one labelled page1 
			     // first object is location of tuple that will be just before the tuple we wish to insert

			}
			 if(tuple.compareTo(min1, clusterColumnName)<0){
				// please discard the bunch of pages to the right side
				j = midOfPages -1;
			}
			else if(tuple.compareTo(min1, clusterColumnName)>0){
				// please discard the bunch of pages to the left side
				i = midOfPages + 1;
			}



		}
		Object [] m = {-1,-1};
		return m; 
		


	} 
	public static int binarysearchkonato2(Page p, Tuple tuple, Table table ){
		Vector<Tuple> tuples1 = p.getTuples();
       String clusterColumnName = table.getClusteringKeyColumn();
		int i =0;
		int j=tuples1.size()-1;
		boolean flag = true;
		while (flag){
			
			int mid = (i+j)/2;
			if(mid == tuples1.size() -1){
				return mid;
			}
			Tuple min1 = tuples1.elementAt(mid);
			Tuple min2 = tuples1.elementAt(mid +1);
			if(tuple.compareTo(min1, clusterColumnName)>0 && tuple.compareTo(min2, clusterColumnName)<0 ){
				// we found location of tuple that will be just before the tuple we want to insert
				return mid;
			}
			if(tuple.compareTo(min1, clusterColumnName)>0){
				i = mid + 1;
			}
			else if(tuple.compareTo(min1, clusterColumnName)<0){
               j = mid ;
			}

		}
		return 0;
	}
	
}
