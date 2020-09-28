import java.util.*;

public class Node {

    // Inputs Hoeffding Tree
    public static final int MAX_EXAMPLES_SEEN = 5;
    public static final int NUMBER_OF_ATTRIBUTES = 4;
    public static final double delta = 0.9;
    public static final double tie_threshold = 0.15;

    // Variables node
    public Integer splitAttr;       // splitting attribute
    public Double splitValue;       // splitting value as returned by calculate_information_gain
    public Integer label;           // the label of the node
    HashMap<Integer, Integer> labelCounts = new HashMap<>(); // the counter of each label
    public Integer nmin;            // Keep tracking the number of samples seen
    public Double information_gain; // the information gain corresponding to the best attribute and the best value
    public Integer setOfAttr;       // set of attributes
    public HashMap<Integer, LinkedList<Integer>> samples = new HashMap<Integer, LinkedList<Integer>>(); // number of values for each column ///
    public ArrayList<Integer> label_List = new ArrayList<>(); // list of labels corresponding to samples of node
    Node leftNode;                 // leftNode
    Node rightNode;                // rightNode
    Node parentNode;               // parentNode

    // left , >=
    // right , <

    // Constructor

    public Node() { }


    // Getter and Setter

    public HashMap<Integer, LinkedList<Integer>> getSamples() { return samples; }

    public HashMap<Integer, Integer> getLabelCounts() {
        return labelCounts;
    }

    public Integer getNmin() { return this.nmin; }

    public int getClassLabel(Node node) { return node.label; }

    public Integer getSplitAttr() { return splitAttr; }

    public Double getSplitValue() { return splitValue; }

    public Double getInformation_gain() { return information_gain; }

    public Integer getSetOfAttr() { return setOfAttr; }

    public ArrayList<Integer> getLabel_List() { return label_List; }

    public Node getLeftNode() { return leftNode; }

    public Node getRightNode() { return rightNode; }

    public Node getParentNode() { return parentNode; }


    // Methods

    /**
     * @param node      - For a given Node
     * @param attribute - For a given Attribute
     * @return - Get the whole list containing the values of this attribute
     */
    public LinkedList getAttributeList(Node node, int attribute) {
        LinkedList<Integer> list = node.samples.get(attribute);
        return list;
    }

    /**
     * @param number_of_attributes <p> Each node has a Samples HashMap and a LabelCounters HashMap
     *                             in order to add new stuff to them, we have to initialize them
     *                             For Samples HashMap:
     *                             We know apriori the number of attributes that we have, so we create as many lists as the number of attributes
     *                             For LabelCounters HashMap:
     *                             We know that we have a binary problem (0:Non Fraud, 1:Fraud) so we need only 2 <0,?> and <1,?> in order to keep track the
     *                             label counts
     */
    public void InitializeHashMapSamplesAndLabelCounts(int number_of_attributes) {

        // Initialize HashMap for Samples
        HashMap<Integer, LinkedList<Integer>> samples = this.getSamples();
        for (int i = 0; i < number_of_attributes; i++) {
            LinkedList<Integer> list = new LinkedList<Integer>();
            samples.put(i, list);
        }

        // Initialize HashMap for LabelCounters
        HashMap<Integer, Integer> labelcounters = this.getLabelCounts();
        labelcounters.put(0, 0);
        labelcounters.put(1, 0);
    }

    /**
     * <p> This function updates the nmin (= how many examples I have seen in a given node so far)
     * This function is called every time a new example is arriving to a node (= not when an examples is passing
     * through a node aka traversal function)
     */
    public void UpdateNMin(Node node) { node.nmin = node.nmin + 1; }

    /**
     * @param node  For a given node
     * @param label For a given label (0 or 1)
     *
     *              <p> This function extracts from a node the LabelsCount HashMap the counter (.get)
     *              and then updates it end put it again back to the HashMap (.put)
     */
    public void UpdateLabelCounters(Node node, Integer label) {
        HashMap<Integer, Integer> labels_hash_map = node.getLabelCounts();
        labels_hash_map.put(label, labels_hash_map.get(label) + 1);
        UpdateNodeClassLabel(node);
    }

    /**
     * @param node Update the class label of a given node comparing the LabelCounts of class 0 and 1
     */
    public void UpdateNodeClassLabel(Node node) {
        HashMap<Integer, Integer> labels_hash_map = node.getLabelCounts();
        int label0 = labels_hash_map.get(0);
        int label1 = labels_hash_map.get(1);
        if (label0 > label1) { node.label = 0; }
        else { node.label = 1; }
    }

    /**
     * @param node For a given node
     * @return whether or not a given node is homogeneous or not
     * <p>
     * If there is a counter in other label which is not 0 then the given node is not homogeneous
     */
    public boolean CheckHomogeneity(Node node) {
        HashMap<Integer, Integer> labels_hash_map = node.getLabelCounts();
        return labels_hash_map.get(0) != 0 || labels_hash_map.get(1) != 0;
    }

    /**
     * <p> This function is responsible for Creating the Hoeffding tree.
     * In theory, it has to be placed when the (state.exits == false) in structured streaming</p>
     */
    public void CreateHT() {
        InitializeHashMapSamplesAndLabelCounts(NUMBER_OF_ATTRIBUTES);
        this.label = null;
        this.information_gain = null;
        this.nmin = 0;
        this.leftNode = null;
        this.rightNode = null;
        this.parentNode = null;
        this.splitAttr = null;
        this.splitValue = null;
        this.setOfAttr = NUMBER_OF_ATTRIBUTES;
    }

    public int TestHT(Node node, String[] sample) {
        Node updatedNode = TraverseTree(node, sample);
        return updatedNode.getClassLabel(node);
    }

    /**
     * </p>This function is responsible for finding the maximum depth of tree
     * @param node root of tree
     * @return "height" of the tree
     */
    int MaxDepth(Node node) {
        if (node == null) { return 0; }
        else {
            // Compute the depth of each subtree
            int lDepth = MaxDepth(node.leftNode);
            int rDepth = MaxDepth(node.rightNode);

            // Use the larger one
            if (lDepth > rDepth) return (lDepth + 1);
            else return (rDepth + 1);
        }
    }

    /**
     * @param node This function clears all the Hoeffding Tree. All it needs is the root
     */
    public void RemoveHT(Node node){
        node.label = null;
        node.information_gain = 0.0;
        node.nmin = 0;
        node.leftNode = null;
        node.rightNode = null;
        node.parentNode = null;
        node.splitAttr = null;
        node.splitValue = null;
        node.setOfAttr = null;
        node.label_List.clear();
        node.samples.clear();
        System.gc();
    }

    public Node FindRoot(Node node){

        if( node.parentNode == null ){ return node; }
        else{
            Node newNode = node.parentNode;
            while( newNode.parentNode != null ){ newNode = newNode.parentNode; }
            return newNode;
        }
    }

    /**
     * @param node For a given Node
     * @return Whether or not a node needs a split
     * The splitting condition is:
     * If a have seen MAX_EXAMPLES_SEEN and the given node is not homogeneous
     */
    public boolean NeedForSplit(Node node) { return node.getNmin() >= MAX_EXAMPLES_SEEN && CheckHomogeneity(node); }
    // Stop splitting based on setOfAttr or entropy of node(=0)


    /**
     * @param node   For a given node
     * @param sample An array of values of attributes aka sample
     *               <p> It is responsible to update the tree. In theory it will be called if the (state.exists == true)
     *               in structured streaming
     *               Jobs:
     *               1. Checks whether or not a split is needed for that given node
     *               2. If not then traverse the tree and finds the node where the given example has to be inserted
     *               3. Inserts the new sample to the node returned from the traversal of the tree.
     */
    public void UpdateHT(Node node, String[] sample){

        Node updatedNode = TraverseTree(node,sample);
        if(NeedForSplit(updatedNode)) {
            AttemptSplit(updatedNode);
            updatedNode = TraverseTree(node,sample);
            InsertNewSample(updatedNode, sample);
        }
        else{ InsertNewSample(updatedNode, sample); }
    }

    /**
     * @param node   For a given node
     * @param sample The sample that has to be added in the given node
     *               <p>
     *               Jobs
     *               1. Add the value of each attribute to the corresponding list of the node
     *               2. Update the labelCounter given the label which comes with the sample
     *               3. Update the nmin - aka that you have added another sample to the node
     */
    public void InsertNewSample(Node node, String[] sample) {
        for (int i = 0; i < sample.length - 1; i++) {
            LinkedList list = getAttributeList(node, i);
            list.add(Integer.parseInt(sample[i]));
        }

        int label = Integer.parseInt(sample[sample.length - 1]);
        node.label_List.add(label);
        UpdateLabelCounters(node, label);
        UpdateNMin(node);
    }

    /**
     * @param node Finds the best attribute
     */
    public void AttemptSplit(Node node){
        double[][] G = FindTheBestAttribute(node); // informationGain,splitAttr,splitValue-row
        double G1 = G[0][0]; // highest information gain
        double G2 = G[0][1]; // second-highest information gain

        // Calculate epsilon
        double epsilon = CalculateHoeffdingBound(node);

        // Attempt split ///
        if ( ( ((G1 - G2) > epsilon) || (G1-G2) < tie_threshold) && G1 != node.information_gain) {
            double[] values = new double[3];
            for (int i = 0; i < 3; i++) { values[i] = G[i][0]; }
            SplitFunction(node, values);
            return;
        }

        // Reset information gain of node if not done the split
        node.information_gain = 0.0;

    }

    /**
     * This function calculates the Hoeffding Bound
     * Hoeffding bound states; Given a random variable r whose range is R ( in our case we have
     * a binary classification problem, so R = logc where c = 2 (number of classes)) and n observations
     * of our examples, the true mean differs from the calculated (by this function) mean at most e(epsilon)
     * with a given probability 1-delta
     */
    public double CalculateHoeffdingBound(Node node) {
        double R = Math.log(2);
        double n = node.getNmin();
        double ln = Math.log(1.0 / delta) / Math.log(2.71828);
        return Math.sqrt((R * R * ln) / 2 * n);
    }

    /**
     * @param node
     * @return <p> Finds the best splitting attribute and splitting value for a given node based on Information Gain</p>
     */
    public double[][] FindTheBestAttribute(Node node){

        double[][] multiples = new double[3][2]; //informationGain,splitAttr,splitValue
        for (int i = 0; i < NUMBER_OF_ATTRIBUTES; i++){

            LinkedList list = getAttributeList(node, i);
            double val[] = new double[list.size()];
            for (int j = 0; j < list.size(); j++) { val[j] = (int) list.get(j); }

            // Calculate splitting value
            Utilities util = new Utilities();
            double splitValues[] = util.Quartiles(val);

            // Calculate informationGain of node for each value and kept the max
            if (i == 0) {
                // GXa = the best attribute and GXb = the second best attribute
                double GXa = InformationGain(node, i, splitValues[0]);
                double GXb = 0.0;
                // Place the best attribute to the 0 position and the second best to the 1 position...
                if (GXa >= GXb) {
                    multiples[0][0] = GXa;
                    multiples[1][0] = i;
                    multiples[2][0] = splitValues[0];
                    multiples[0][1] = GXb;
                    multiples[1][1] = i + 1;
                    multiples[2][1] = splitValues[1];
                    // ... else to the opposite
                }
                else {
                    multiples[0][1] = GXa;
                    multiples[1][1] = i;
                    multiples[2][1] = splitValues[0];
                    multiples[0][0] = GXb;
                    multiples[1][0] = i + 1;
                    multiples[2][0] = splitValues[1];
                }
                // In case the current attribute is not the first (0) attribute
            }
            else {
                // Get the G of the first quartile...
                double tempG = InformationGain(node, i, splitValues[0]);
                // If the tempG is greater from the already best G, put the tempG in the 0 position and the previous G
                // to the second.. The previous second has to be overwritten and therefore discarded
                if (tempG > multiples[0][0]) {
                    multiples[0][1] = multiples[0][0];
                    multiples[1][1] = multiples[1][0];
                    multiples[2][1] = multiples[2][0];

                    multiples[0][0] = tempG;
                    multiples[1][0] = i;
                    multiples[2][0] = splitValues[0];
                }
                // if the tempG is less from the best attribute BUT greater than the second... we put the tempG to the
                // 1 position and keep the already best to the 0 position
                else if (tempG > multiples[0][1]) {
                    multiples[0][1] = tempG;
                    multiples[1][1] = i;
                    multiples[2][1] = splitValues[0];
                }
                // else do nothing
            }
            for (int j = 1; j < splitValues.length; j++) {
                double tempG = InformationGain(node, i, splitValues[j]);
                if (tempG > multiples[0][0]) {
                    multiples[0][1] = multiples[0][0];
                    multiples[1][1] = multiples[1][0];
                    multiples[2][1] = multiples[2][0];
                    multiples[0][0] = tempG;
                    multiples[1][0] = i;
                    multiples[2][0] = splitValues[j];
                }
                else if (tempG > multiples[0][1]) {
                    multiples[0][1] = tempG;
                    multiples[1][1] = i;
                    multiples[2][1] = splitValues[j];
                }
            }
        }
        return multiples;
    }

    public Node TraverseTree(Node node,String[] sample){

        if( node.leftNode == null && node.rightNode == null ){ return node; }
        else{
            //Left child node
            if ( node.splitValue.doubleValue() <= Double.parseDouble(sample[node.splitAttr]) ) { return TraverseTree(node.leftNode,sample); }
            //Right child node
            else { return TraverseTree(node.rightNode,sample); }
        }
    }

    public void SplitFunction(Node node,double[] values){

        // Generate nodes
        Node child1 = new Node();
        Node child2 = new Node();

        // Initialize parent and child nodes
        child1.parentNode = node;
        child2.parentNode = node;
        node.leftNode = child1;
        node.rightNode = child2;

        // Initialize informationGain,splitAttribute,splitValue
        node.information_gain = values[0];
        node.splitAttr = (int)values[1];
        node.splitValue = values[2];

        // Initialize nmin,information_gain,label
        child1.nmin = 0;
        child2.nmin = 0;
        child1.information_gain = 0.0;
        child2.information_gain = 0.0;
        child1.label = null;
        child2.label = null;

        // Initialize set of attributes
        child1.setOfAttr = node.setOfAttr - 1;
        child2.setOfAttr = child1.setOfAttr;

        child1.InitializeHashMapSamplesAndLabelCounts(NUMBER_OF_ATTRIBUTES);
        child2.InitializeHashMapSamplesAndLabelCounts(NUMBER_OF_ATTRIBUTES);

        //Clear samples,labelCounts,label_List,setOfAttr on parent node
        node.labelCounts.clear();
        node.samples.clear();
        node.label_List.clear();
        node.setOfAttr=null;

    }

    public double InformationGain(Node node,int splitAttr,double splitValue){

        // Calculate count for label 0,1
        int labelCount0Up=0;
        int labelCount1Up=0;
        int labelCount0Low=0;
        int labelCount1Low=0;
        for(int i=0;i<node.samples.get(splitAttr).size();i++){
            if( (double)node.samples.get(splitAttr).get(i) >= splitValue){
                if (node.label_List.get(i) == 0) { labelCount0Up++; }
                else { labelCount1Up++; }
            }
            else{
                if (node.label_List.get(i) == 0) { labelCount0Low++; }
                else { labelCount1Low++; }
            }
        }

        // Calculate entropy node
        double log0 = Math.log((double)node.labelCounts.get(0)/node.nmin) / Math.log(2);
        double log1 = Math.log((double)node.labelCounts.get(1)/node.nmin) / Math.log(2);
        if( node.labelCounts.get(0) == 0 ){ log0 = 0;}
        else if( node.labelCounts.get(1) == 0){ log1 = 0;}
        double entropyNode = (-1)*(((double)node.labelCounts.get(0)/node.nmin)*log0) + (-1)*(((double)node.labelCounts.get(1)/node.nmin)*log1);

        // Update information_gain node
        node.information_gain = entropyNode; ///

        // Calculate entropy based on splitAttr,for left and right node
        // Left Node
        double entropyLeftNode;
        int totalCountLeftNode = labelCount0Up+labelCount1Up;
        log0 = Math.log((double)labelCount0Up/totalCountLeftNode) / Math.log(2);
        log1 = Math.log((double)labelCount1Up/totalCountLeftNode) / Math.log(2);
        if( labelCount0Up == 0){ log0 = 0; }
        else if( labelCount1Up == 0 ){ log1 = 0;}

        if( totalCountLeftNode == 0) { entropyLeftNode = 0; }
        else { entropyLeftNode = (-1)*(((double) labelCount0Up / totalCountLeftNode)*log0) + (-1)*(((double) labelCount1Up / totalCountLeftNode)*log1); }

        // Right Node
        double entropyRightNode;
        int totalCountRightNode = labelCount0Low+labelCount1Low;
        log0 = Math.log((double)labelCount0Low/totalCountRightNode) / Math.log(2);
        log1 = Math.log((double)labelCount1Low/totalCountRightNode) / Math.log(2);
        if( labelCount0Low == 0){ log0 = 0; }
        else if( labelCount1Low == 0 ){ log1 = 0;}

        if( totalCountRightNode == 0) { entropyRightNode = 0; }
        else{ entropyRightNode = (-1)*(((double)labelCount0Low/totalCountRightNode)*log0) + (-1)*(((double)labelCount1Low/totalCountRightNode)*log1); }

        //Calculate weighted average entropy of splitAttr
        double weightedEntropy = (((double)totalCountLeftNode/node.nmin)*entropyLeftNode) + (((double)totalCountRightNode/node.nmin)*entropyRightNode);

        return entropyNode-weightedEntropy;

    }

}
