
// import java.util.HashMap;
// import java.util.Hashtable;

// import trial.Page;
// import trial.Serializer;
// import trial.Table;
// import trial.Tuple;

// public class Run {
//     public static void main(String[] args) throws Exception {
//     	//testing update
//     	/*HashMap<String, Object> hm = new HashMap<String, Object>();
//     	hm.put("id","1221");
//     	hm.put("name","ahmed");
//     	hm.put("uni","guc");
//     	hm.put("age","21");
//     	Tuple tuple = new Tuple(hm);
//     	System.out.println(tuple);
//     	Hashtable<String, Object> ht = new Hashtable<String, Object>();
//     	ht.put("name","engy");
//     	ht.put("age","65");
//     	tuple.updateTuple(ht);
//     	System.out.println(tuple);*/
//     	//testing linear search
//     	DBApp db = new DBApp();
//     	Hashtable<String, String> ht = new Hashtable<String, String>();
//     	ht.put("id", "java.lang.Integer");
//     	ht.put("name", "java.lang.String");
//     	ht.put("gpa", "java.lang.Double");
//     	db.createTable("Student", "id", ht);
// 		Table table = (Table) Serializer.deSerialize("Student");
// 		table.setClusterKey("id");
// 		Serializer.serialize(table, table.getTableName());

//     	Page page = table.createPage();
//     	Page p2 = table.createPage();
//     	Page p3 = table.createPage();
//     	Page p4 = table.createPage();
//     	HashMap<String, Object> hm1 = new HashMap<String, Object>();
//     	HashMap<String, Object> hm2 = new HashMap<String, Object>();
//     	HashMap<String, Object> hm3 = new HashMap<String, Object>();
//     	HashMap<String, Object> hm4 = new HashMap<String, Object>();
//     	HashMap<String, Object> hm5 = new HashMap<String, Object>();

//     	hm1.put("id", "1221");
//     	hm1.put("name", "ahmed");
//     	hm1.put("gpa", 3.5);
//     	Tuple t1 = new Tuple(hm1);
//     	page.addTuple(t1);

    	
//     	hm2.put("id", "1222");
//     	hm2.put("name", "ahmed");
//     	hm2.put("gpa", 3.2);
//     	Tuple t2 = new Tuple(hm2);
//     	p2.addTuple(t2);
    	
//     	hm3.put("id", "2076");
//     	hm3.put("name", "ali");
//     	hm3.put("gpa", 3.1);
//     	Tuple t3 = new Tuple(hm3);
//     	p3.addTuple(t3);
    	
//     	hm4.put("id", "3055");
//     	hm4.put("name", "adam");
//     	hm4.put("gpa", 2);
//     	Tuple t4 = new Tuple(hm4);
//     	p4.addTuple(t4);
    	
//     	hm5.put("id", "7000");
//     	hm5.put("name", "engy");
//     	hm5.put("gpa", 0);
//     	Tuple t5 = new Tuple(hm5);
//     	p4.addTuple(t5);
    	
//     	Serializer.serialize(page, "Student1");
//     	Serializer.serialize(p2, "Student2");
//     	Serializer.serialize(p3, "Student3");
//     	Serializer.serialize(p4, "Student4");
    	
//     	Serializer.serialize(table,table.getTableName());

    	
//     	Hashtable<String, Object> hts = new Hashtable<String, Object>();
//     	hts.put("name", "shiko");
//     	//hts.put("id", "1221");
//     	//System.out.println("");
//     	//System.out.println(p4);
//     	//db.updateTable("Student", "engy", hts);
//     	//Page pNew = (Page) Serializer.deSerialize("Student4");
//     	//System.out.println("blabla");
//     	System.out.println(Serializer.deSerialize("Student"));
//     	//db.deleteFromTable("Student", hts);
//     	db.updateTable("Student", "7000", hts);
//     	System.out.println("");
//     	System.out.println(Serializer.deSerialize("Student"));

//     	//System.out.println(db.linearSearch("Student", hts));
//     	//System.out.print("Result found: " + db.binarySearch("Student", "name", "ahmed"));
    	
//     }
// }
