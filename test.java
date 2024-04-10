	public void insertIntoTable(String strTableName, 
                                Hashtable<String,Object>  htblColNameValue) throws DBAppException{
								Table t;
								if(!Serializer.wasSerialized(strTableName)){
									throw new DBAppException("Table does not exist");
								}
								try {
									t = (Table) Serializer.deSerialize(strTableName);
								} catch (Exception e) {
									throw new DBAppException("An error occured while deserializing the table");
								}



                                Hashtable<String,String> hash = t.getIndexHash();  //column name , Index name
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
									System.out.println("strTableName columnName");
									System.out.println(strTableName+" "+columnName);
									for(int i=0;i<meta.size();i++){
										String tableNameMeta = meta.get(i).get(0);
										String columnNameMeta = meta.get(i).get(1);
										System.out.println("tableNameMeta columnNameMeta");
										System.out.println(tableNameMeta+" "+columnNameMeta);
										if(tableNameMeta.compareTo(strTableName)==0 && columnNameMeta.compareToIgnoreCase(columnName)==0){
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

							//no index on the primary key --> insert normally


                                    Object [] binarySearchReturn = binarysearchkonato(t, tupletoinsert);

						// 3 return cases in binarysearchkonato: 
						// 	 	1) string: this will be the first tuple to insert
						//  	2) contains -1 : my tuple's PK is the smallest PK 
						//  	3) normal ({rakameltuple, lastpage1, lastpage, rakamelpage})
		                            Page firstPage;
									if (binarySearchReturn[0] instanceof String) {
										try{
											 firstPage = t.createPage();
											
										}
										catch (Exception ex){
                                              throw new DBAppException("An error occured while creating the first page");
											  
										}
                                         try{
											Page despage = (Page) Serializer.deSerialize(firstPage.fileName);
											thedesiredpagename = despage.fileName;
											String ourinsertedrow = despage.addTuple(tupletoinsert);  //return string won't be used
											//serialize the page
											Serializer.serialize(despage, despage.fileName);
											
										}

										catch(Exception e){
											throw new DBAppException("An error occured while serializing or deserializing the page"); 
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

								else{
									//there exist an index on the primary key column that will be used in the insertion

									//recall: Hashtable<String,Index> hash = t.getIndexHash();  //column name , Index
									

									String primarykey = t.getClusteringKeyColumn();

									//table has an index on the primary key column --> we'll use it in the insertion
										String indexName = hash.get(primarykey);
										Index clusteringindex;
										try {
											clusteringindex = (Index) Serializer.deSerialize(indexName);
										} catch (Exception e) {
											// TODO: handle exception
											throw new DBAppException("An error occured while deserializing the index");
										}
									   
									   bplustree bp = clusteringindex.getBTree();
									   Object VTpInBt = tupletoinsert.getValue(primarykey);  // value of the PK column in the tuple to insert
									   Comparable comp = (Comparable) VTpInBt;
                                       thedesiredpagename = bplustree.helperforindexsearch(bp,comp);
									    if(thedesiredpagename.equals("we are going to insert our tuple in the last page if not full handled in DB APP")){  //if rightsibling = null
											try{
												String thelastpagename = t.getPageFileNames().get((t.getPageFileNames()).size() -1);
												int rakamAkherPage = t.getPageFileNames().size()-1;
												System.out.println("252");
												Page thelastpage = (Page) Serializer.deSerialize(thelastpagename);
												Serializer.serialize(thelastpage, thelastpage.fileName);
												int tupleToInsertAfter = binarysearchkonato2(thelastpage, tupletoinsert, t); //we found the page to insert into before the try but we still need to find our tuple's location in that page
												thedesiredpagename =  insertinpageskonato(t,tupletoinsert,rakamAkherPage,tupleToInsertAfter); //holds the page name that stores our tuple
											}
											catch(Exception ex){
												throw new DBAppException("An error occured while serializing or deserializing the page");
											}
											
									   }
									   else{  //right sibling != null        so thedesiredpagename holds the page name (String) that we'll insert the tuple into
											int rakamofthepage = 0;
											int i = 0;
											while(t.getPageFileNames().get(i)!= thedesiredpagename){
													i = i + 1;
													rakamofthepage = rakamofthepage +1;
											}
											Page thedesiredpage;
											
											try{
												System.out.println("273");
												thedesiredpage = (Page) Serializer.deSerialize(thedesiredpagename);
												Serializer.serialize(thedesiredpage, thedesiredpage.fileName);
												int rakamofthetuple = binarysearchkonato2(thedesiredpage, tupletoinsert, t);
												String notimportantstring =  insertinpageskonato(t,tupletoinsert,rakamofthepage,rakamofthetuple);
											}
											catch(Exception ex){
												throw new DBAppException("An error occured while serializing or deserializing the page");
											}
									   }

									   try{
										Serializer.serialize(clusteringindex, indexName);
									   }
									   catch(Exception ex){
										throw new DBAppException("An error occured while serializing the index: " + indexName);
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

								//5alasna inserting fel pages (either normally or using PK index)

								// now we'll update all indexes we have and serialize them

								for (Map.Entry<String, String> entry : hash.entrySet()) {
									String columnName = entry.getKey(); // Getting Key (column name)
									String indexName = entry.getValue();
									try {
										index = (Index) Serializer.deSerialize(indexName);
									} catch (Exception e) {
										// TODO: handle exception
										throw new DBAppException("error deserializing: " + indexName);
									}
									bplustree tree = index.getBTree();
									Comparable o1;


									//checking that the column that you have it's index is the same as the column you'll insert into it's index
									for (Map.Entry<String, Object> entry1 : htblColNameValue.entrySet()) {
										String column = entry1.getKey(); // Getting Key (column name)
										o1 = (Comparable)entry1.getValue();
										if(column.equals(columnName)){
											 Vector<String> vectorofpages = tree.search(o1);
											 if(vectorofpages == null){
                                                tree.insert(o1, thedesiredpagename);
											 }
											 else{
												//if(!vectorofpages.contains(thedesiredpagename)){
											 		vectorofpages.add(thedesiredpagename);
												//}
											 }
											
											break;
										}
									}
									try{
										Serializer.serialize(index, indexName);
									}
									catch(Exception e){
										throw new DBAppException("An error occured while serializing the index: " + indexName);
									}
									

								}
                                
                            
                                
      //  throw new DBAppException("not implemented yet");
    }
