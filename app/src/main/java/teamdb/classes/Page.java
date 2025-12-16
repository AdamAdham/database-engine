package teamdb.classes;
import java.io.IOException;
import java.io.Serializable;
import java.util.Vector;

//import javax.management.ValueExp;

public class Page implements Serializable,Cloneable{
    private int tupleNum;
    private int maxRows; //Put in DBApp not necc for page AA
    private Vector<Tuple> tuples;
    public String fileName; //TODO private
 
    public Page(String filename) throws IOException, DBAppException{
        ConfigReader conf = new ConfigReader();
        maxRows = conf.getMaxRows();
        Serializer.serialize(this, filename);
        tupleNum = 0;
        this.fileName = filename;
        tuples = new Vector<Tuple>();
    }

    public String addTuple(Tuple tuple){
        if(tupleNum > maxRows){
            return "Fakes"; // Add exception AA
        }
        tuples.add(tuple);
        tupleNum++;
        return tuple.toString();
    }

    public void setTuple(int i, Tuple t) {
    	tuples.set(i, t);
    }

    

    public String toString(){
        // String str = "";
        // for(int i=0;i<tuples.size();i++){
        //     str += ", " + tuples.elementAt(i).toString(); // Would like to add a break but not said in decription
        // }
        String temp = tuples.toString();
        String finalStr = temp.substring(3, temp.length() - 4);//remove the
        return finalStr;
    }

    public Vector<Tuple> getTuples(){
        return tuples;
    }

    public int getMaxRows(){
        return maxRows;
    }

    public void setTuples(Vector<Tuple> tuples) {
    	this.tuples = tuples;
    }

    public boolean isEmpty() {
        return tuples.isEmpty();
    }

    
    public String getFileName(){
        return this.fileName;
    }

    //For testing purposes
    // TODO remove method and main when submitting

    // public Iterator<Tuple> selectFromTable(Vector<Tuple> tuples){
    //     ArrayList<Tuple> list = new ArrayList<>();
    //     list.add(tuples.get(0));
    //     list.add(tuples.get(5));
    //     list.add(tuples.get(8));
    //     return list.iterator();
    // }

    // public static void main(String[] args) {
    //     Page page;
    //     try {
    //         page = new Page("Page");
    //     } catch (Exception e) {
    //         // TODO: handle exception
    //         return;
    //     }
        
    //     for(int i=0;i<10;i++){
    //         HashMap hash = new HashMap<>();
    //         hash.put("name","zhangsan"+i);
    //         hash.put("id", i);
    //         Tuple tuple = new Tuple(hash);
    //         page.addTuple(tuple);
    //     }
    //     Iterator<Tuple> it = page.selectFromTable(page.getTuples());
    //     while (it.hasNext()) {
    //         Tuple tuple = it.next();
    //         // Process each tuple as needed
    //         System.out.println(tuple);  // For example, print each tuple
    //     }
    // }
}
