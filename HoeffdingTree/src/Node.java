import java.util.*;
import java.util.stream.Collectors;

public class Node {

    String splitAttr;
    String splitValue;

    Node parentNode;
    ArrayList<Node> childNode = new ArrayList<>();

    int samples;
    String label;

    // Position 0 for No
    // Position 1 for Yes
    HashMap<String,Integer> labelCounts = new HashMap<>();

    // ArrayList<String> setOfAttributes = new ArrayList<>();

    HashMap<String, List<HashMap<String, Integer>>> attr = new HashMap<>();

    //left -> true , <=
    //right -> false , >

    //Constructors
    public Node(){ }

    public Node(String splitAttr,String splitValue){
        this.splitAttr = splitAttr;
        this.splitValue = splitValue;
    }


    //Getter and Setter
    public String getSplitAttr() { return splitAttr; }

    public void setSplitAttr(String splitAttr) { this.splitAttr = splitAttr; }

    public String getSplitValue() { return splitValue; }

    public void setSplitValue(String splitValue) { this.splitValue = splitValue; }

    public Node getParentNode() { return parentNode; }

    public void setParentNode(Node parentNode) { this.parentNode = parentNode; }

    public int getSamples() { return samples; }

    public void setSamples(int totalCount) { this.samples = samples; }


    // Methods
    public void splitNode(Node node,int attribute,String value){

        // Generate nodes
        Node child1 = new Node();
        Node child2 = new Node();

        // Initialize parent and child nodes
        child1.parentNode = node;
        child2.parentNode = node;
        node.childNode.add(child1);
        node.childNode.add(child2);

        // Initialize labelCounts and samples
        // Child_1 and child_2
        for( Iterator<String> keys = node.attr.keySet().iterator(); keys.hasNext(); ){
            String key=keys.next();
            child1.labelCounts.put(key,node.attr.get(key).get(attribute).get(value));
            child2.labelCounts.put(key,node.attr.get(key).get(attribute).values().stream().collect(Collectors.summingInt(Integer::intValue)) - child1.labelCounts.get(key));
        }

        // Initialize samples
        child1.samples = child1.labelCounts.values().stream().collect(Collectors.summingInt(Integer::intValue));
        child2.samples = node.samples - child1.samples;

        // Initialize label and nijk counts
        child1.label =  child1.labelCounts.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toList()).get(child1.labelCounts.size()-1).getKey();
        child2.label =  child2.labelCounts.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toList()).get(child2.labelCounts.size()-1).getKey();
        //Collections.sort((ArrayList<Integer>)child1.labelCounts.values());

    }

}

