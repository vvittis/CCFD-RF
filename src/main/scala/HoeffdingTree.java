public class HoeffdingTree {

    public double instances_seen;
    public double correctly_classified;
    public double weight;
    public int[] m_features; // list of labels corresponding to samples of node
    public Node root = new Node();


    public void CreateHoeffdingTree(int m_features, int Max, int max_examples_seen, double delta, double tie_threshold) {
        root.CreateHT(m_features, max_examples_seen, delta, tie_threshold);
        instances_seen = 0.0;
        correctly_classified = 0.0;
        weight = 1.0;
        initialize_m_features(m_features, Max);
    }

    public void UpdateHoeffdingTree(Node node, String[] input){
        String[] selectedInput = this.select_m_features(input);
        node.UpdateHT(node, selectedInput);
    }

    public int TestHoeffdingTree(Node node, String[] input, int keyTuple){

        int predicted_value = 0;
        String[] selectedInput = this.select_m_features(input);

        if (keyTuple == -5 || keyTuple == -10) { predicted_value = node.TestHT(node, selectedInput); }
        else {
            this.instances_seen++;
            if (this.instances_seen == 1) { setWeight(1.0, this.instances_seen); }
            else {
                predicted_value = node.TestHT(node, selectedInput);
                if (predicted_value == Integer.parseInt(selectedInput[selectedInput.length - 1])) { this.correctly_classified++; }
                this.setWeight(this.correctly_classified, this.instances_seen);
            }
        }
        return predicted_value;
    }

    public Node FindRoot(Node node) { return node.FindRoot(node); }

    public void setWeight(double correctly_classified, double instances_seen) {
        this.weight = correctly_classified / instances_seen;
    }

    public double getWeight(){ return this.weight; }

    /**
     * @param m   how many features I want the Hoeffding Tree to have
     * @param Max What is the range aka how many features I have to select from
     */
    public void initialize_m_features(int m, int Max) {
        Utilities util = new Utilities();
        this.m_features = util.ReservoirSampling(m, Max);
    }

    public String[] select_m_features(String[] input_string) {

        String[] output_string = new String[this.m_features.length + 1];
        for (int i = 0; i < this.m_features.length; i++) { output_string[i] = input_string[this.m_features[i]]; }

        output_string[this.m_features.length] = input_string[input_string.length - 1];

//        for(int i=0; i< output_string.length-1; i++) {
//            System.out.print(this.m_features[i]+" : "+output_string[i] + " ");
//        }
//        System.out.print(this.m_features[this.m_features.length-1]+1+" : "+output_string[output_string.length-1] + " ");
//        System.out.println("");
        return output_string;
    }

    public void print_m_features() {
        System.out.println();
        for (int m_feature : this.m_features) { System.out.print(m_feature); }
        System.out.println();
    }

}


