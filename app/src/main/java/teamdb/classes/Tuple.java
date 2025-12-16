package teamdb.classes;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class Tuple implements Serializable{
    private HashMap<String, Object> values;
    private Iterator<Map.Entry<String, Object>> iterator;

    public Tuple(HashMap<String,Object> keys){
        this.values = keys;
        this.iterator = values.entrySet().iterator();
    }

    // Method to get the value of a specific column
    public Object getValue(String columnName) {
        return values.get(columnName);
    }

    public String toString(){
        String str = "";
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            //String columnName = entry.getKey(); // Getting Key
            Object value = entry.getValue();
            str += "," + value;
        }
        return str;
    }

    @SuppressWarnings({ "unchecked"})
    public int compareTo(Tuple t,String clusteringKey){
        // Assuming we have done the 
        Comparable value1 = (Comparable) values.get(clusteringKey);
        Comparable value2 = (Comparable) t.values.get(clusteringKey);
        return value1.compareTo(value2);
    } 

    public HashMap<String, Object> getValues(){
        return values;
    }

     public Tuple updateTuple(Hashtable<String, Object> ht) {
    	// values.forEach((oldKey,oldValue)
    	// 		-> {
    	// 			if(ht.get(oldKey) != null) {
    	// 				values.replace(oldKey, ht.get(oldKey));
    	// 				System.out.print(ht.get(oldKey));
    	// 			}
    	// 		}
    	// 		);
        // unnavailable in this version (DK why)
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            String oldKey = entry.getKey();
        
            if (ht.get(oldKey) != null) {
                values.replace(oldKey, ht.get(oldKey));
                System.out.print(ht.get(oldKey));
            }
        }
    	return this;
    }

    // @Override
    // public boolean hasNext() {
    //     return iterator.hasNext();
    // }

    // @Override
    // public Object next() {
    //     if (iterator.hasNext()) {
    //         Map.Entry<String, Object> entry = iterator.next();
    //         return entry.getValue();
    //     } else {
    //         throw new UnsupportedOperationException("No more elements in Tuple");
    //     }
    // }

    public static void main(String[] args) throws IOException, DBAppException {
        System.out.println("Hello world");
        HashMap<Integer,Integer> hm = new HashMap<Integer,Integer>();
        Serializer.serialize(hm, "blabla.class");
    }
}
