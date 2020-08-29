import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class main {

    public static void main(String[] args) throws IOException {

        Node newNode = new Node();
        int firstLine=0;

        BufferedReader reader = new BufferedReader(new FileReader("data.txt"));
        String line = reader.readLine();
        while (line != null ){

            line = reader.readLine();
            if(line == null){ break; }
            String[] arrOfStr = line.split(" ");

            // Update samples of newNode
            newNode.samples++;

            // Initialize nijk counts
            List<HashMap<String, Integer>> list = newNode.attr.get(arrOfStr[3]);
            HashMap<String, Integer> attr = new HashMap<>();
            if( list==null ){
                list = new ArrayList<>(3);
                firstLine=1;
            }
            else{ firstLine=0; }

            // Increments labelCounts
            newNode.labelCounts.compute(arrOfStr[3], (k, v) -> (v == null) ? 1 : v + 1);

            // Increments nijk counts
            for(int i=0;i<arrOfStr.length-1;i++) {
                if(firstLine==1){
                    attr.compute(arrOfStr[i], (k, v) -> (v == null) ? 1 : v + 1);
                    list.add(i, (HashMap<String, Integer>) attr.clone());
                    attr.clear();
                    continue;
                }
                list.get(i).compute(arrOfStr[i], (k, v) -> (v == null) ? 1 : v + 1);
            }

            // Label l with the majority class among the examples seen so far at l
            // If the examples seen so far at l are not all of the same class/label, then compute G-function and select the attribute
            newNode.attr.put(arrOfStr[3],list);

            // Splitting Condition(temp)
            if(newNode.labelCounts.size() != 1){ System.out.println("Examples are not all of the same class!!!"); }

            // After selection,split the node
            // Let be the Outlook(pos=0)-Rainy the first attribute
            // Initialize splitAttr and splitValue
            if(line.equals("Sunny Mild High No")){ // temporary

                // Declare label of node
                newNode.label =  newNode.labelCounts.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toList()).get(newNode.labelCounts.size()-1).getKey();

                // Split node
                newNode.splitNode(newNode,0,"Rainy");
            }

        }

        reader.close();

    }

}
