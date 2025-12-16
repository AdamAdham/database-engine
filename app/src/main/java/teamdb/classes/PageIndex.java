package teamdb.classes;
import java.util.ArrayList;
import java.util.Hashtable;

    public class PageIndex {
        
        public Hashtable<String, ArrayList<Integer>> hashTable = new Hashtable<String, ArrayList<Integer>>();
        
        public PageIndex() {
        }
        public PageIndex(Hashtable<String, ArrayList<Integer>> ht) {
            this.hashTable = ht;
        }
        public String toString() {
            return hashTable.toString() + "\n";
        }

        public Hashtable<String, ArrayList<Integer>> getHashTable() {
            return hashTable;
        }
    }
