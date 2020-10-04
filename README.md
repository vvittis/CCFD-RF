# CCFD-RF

This is a project from Credit Card Fraudulent Detection with Random Forest using Spark Structured Streaming

# To-Do
1. **Online Bagging:** We have to use, inside the function _FlatMapWithState()_, the _Poisson(lambda = 6)_ function in order to know the number of time each tree will 
be trained with the same input instance.
2. **Randomly select k-features:** Inside the State when the state does not exist we have to randomly select k features in order to initialize the Hoeffding tree the k
has to be an input to the system   
3. **Id Nodes Number Tracking:** In case of a node split, we have to give each child a new id.
4. **Weighted-voting:** This prevents us from having lazy tree estimators. We assume that a tree _l_ has seen nl instances and has correctly classified cl instances.
The weight of his next vote will be _cl/nj_
5. **Test - Train Concept:** We have to change the concept from only training and only testing to test-train and testing. When a new training instance is feeded to the
system, we do fist the testing in order to update its tree the voting weight and then train each tree.
6. **Warning-Drift Detection:** Find the condition for warning and drift detection
7. **N-number of trees:** We have to add N as an input to the system in order to specify the number of trees.
8. **Change the case class:** Transform the case class with String class. Anyway the hoeffding tree gets a string input. No reason to have a case class.
9. **Finish the Power-Point presentation:**

The execution plan which is broken into stages where things can be processed in parallel and do not have a shuffle involved 

and then stages are broken down to tasks
that are distributed to individual nodes on your cluster. 


Licensed under the [MIT Licence](LICENSE).