package teamdb.classes;

import java.security.Key;
import java.util.Vector;

public class AdamHelpers {
    public static Pair<Page,Integer> binarySearchRangeAdam(Table table, Comparable key,Boolean upperBound,Boolean inclusive){
		String clusterColumnName = table.getClusteringKeyColumn();
		Vector<String> pageFileNames = table.getPageFileNames();
		int i=0;
		int j=pageFileNames.size()-1;
		while(i<=j){
			//Calculate midpoint of search for all pages
			int midOfPages = (i+j)/2;
			String pageName = pageFileNames.get(midOfPages);
			Page page;
			try {
				page = (Page) Serializer.deSerialize(pageName);
			} catch (Exception e) {
				// TODO: handle exception
				return null;
			}
			Vector<Tuple> tuples = page.getTuples();
			int a=0;
			Tuple min = tuples.elementAt(a);
			int b=tuples.size()-1;
			Tuple max = tuples.elementAt(b);
			Comparable minValue = (Comparable) min.getValue(clusterColumnName);
			Comparable maxValue = (Comparable) max.getValue(clusterColumnName);
			if(key.compareTo(minValue)<0){
				// If less than min in page then discard right half of pages
				j = midOfPages-1;

			}else if(key.compareTo(maxValue)>0){
				// If greater than max in page then discard left half of pages
				i = midOfPages+1;
			}
			else{
			// Is in page
			int index = -1;
				while(a<=b){
					// Normal binary search
					// Midpoint of all tuples in this page
					int midTuple = (a+b)/2;
					Tuple currentTuple = tuples.elementAt(midTuple);
					Comparable currentValue = (Comparable)currentTuple.getValue(clusterColumnName);
					if(key.compareTo(currentValue)==0){
						index = (a+b)/2;
					}
					if(key.compareTo(currentValue)<0){
						// If less than min in page then discard right half of pages
						b = midTuple-1;
		
					}else if(key.compareTo(currentValue)>0){
						// If less than min in page then discard right half of pages
						a = midTuple+1;
					}				
				}	
				if(index==-1){
					index = a; // Point to the index that the key would be placed
				}
			}	
			return new Pair<Page,Integer>(page, a);
		}
		return null;
	}

	public static Tuple binarySearchAdam(Table table, Comparable key){
		String clusterColumnName = table.getClusteringKeyColumn();
		Vector<String> pageFileNames = table.getPageFileNames();
		int i=0;
		int j=pageFileNames.size()-1;
		while(i<=j){
			//Calculate midpoint of search for all pages
			int midOfPages = (i+j)/2;
			String pageName = pageFileNames.get(midOfPages);
			Page page;
			try {
				page = (Page) Serializer.deSerialize(pageName);
			} catch (Exception e) {
				// TODO: handle exception
				return null;
			}
			Vector<Tuple> tuples = page.getTuples();
			int a=0;
			Tuple min = tuples.elementAt(a);
			int b=tuples.size()-1;
			Tuple max = tuples.elementAt(b);
			Comparable minValue = (Comparable) min.getValue(clusterColumnName);
			Comparable maxValue = (Comparable) max.getValue(clusterColumnName);
			if(key.compareTo(minValue)<0){
				// If less than min in page then discard right half of pages
				j = midOfPages-1;

			}else if(key.compareTo(maxValue)>0){
				// If greater than max in page then discard left half of pages
				i = midOfPages+1;
			}
			else{
			// Is in page
				while(a<=b){
					// Normal binary search
					// Midpoint of all tuples in this page
					int midTuple = (a+b)/2;
					Tuple currentTuple = tuples.elementAt(midTuple);
					Comparable currentValue = (Comparable)currentTuple.getValue(clusterColumnName);
					if(key.compareTo(currentValue)==0){
						return currentTuple;
					}
					if(key.compareTo(currentValue)<0){
						// If less than min in page then discard right half of pages
						b = midTuple-1;
		
					}else if(key.compareTo(currentValue)>0){
						// If less than min in page then discard right half of pages
						a = midTuple+1;
					}				
				}	
			}	
		}
		return null;
	}

	

	public static int binarySearchPageAdam(Page page, String columnName,Comparable<Key> k) {
		// Return type int because we never binary search unless we have the clusterColumn as the columnName so no duplicates
		Vector<Tuple> tuples = page.getTuples();
		int a=0;
		int b=tuples.size()-1;
		while(a<=b){
			// Normal binary search
			// Midpoint of all tuples in this page
			int midTuple = (a+b)/2;
			Tuple currentTuple = tuples.elementAt(midTuple);
			if(currentTuple.compareTo(currentTuple, columnName)==0){
				return (a+b)/2;
			}
			if(currentTuple.compareTo(currentTuple, columnName)<0){
				// If less than min in page then discard right half of pages
				b = midTuple-1;

			}else if(currentTuple.compareTo(currentTuple, columnName)>0){
				// If less than min in page then discard right half of pages
				a = midTuple+1;
			}				
		}	
		// Temp until we know what we want to do with
		return -1;
	}

	public static Vector<Integer> linearSearchPageAdam(Vector<Tuple> tuples, String columnName,Comparable<Key> k) {
		Vector<Integer> indeces = new Vector<Integer>();
		for(int i=0;i<tuples.size();i++){
			if(tuples.elementAt(i)==null) continue;
			else{
				if(tuples.elementAt(i).getValues().get(columnName)==k){
					indeces.add(i);
				}
			}
		}
		return indeces;
	}
}
