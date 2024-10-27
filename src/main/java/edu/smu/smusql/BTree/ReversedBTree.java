package edu.smu.smusql.BTree;

import java.util.ArrayList;
import java.util.List;


class ReversedBTreeNode {
    KeyValuePair<Object, List<Object>>[] keys; // Array to store new key-value pairs (Value -> List of Keys)
    int t; // Minimum degree (defines the range for number of keys)
    ReversedBTreeNode[] children; // Array to store child pointers
    int n; // Current number of keys
    boolean leaf; // True when node is leaf, else False

    public ReversedBTreeNode(int t, boolean leaf) {
        this.t = t;
        this.leaf = leaf;

        keys = new KeyValuePair[2 * t - 1];
        children = new ReversedBTreeNode[2 * t];
        n = 0;
    }

    // Function to insert a new value as a key, and the key as a value (for multiple keys, store in a list)
    public void insertNonFull(KeyValuePair<Object, Object> KV) {
        int i = n - 1;

        if (leaf) {
            // Find correct location for the new key (which is the original value)
            while (i >= 0 && ((Comparable) KV.getKey()).compareTo(keys[i].getKey()) < 0) {
                keys[i + 1] = keys[i];
                i--;
            }

            // If the key (original value) already exists, append the original key to the list
            if (i >= 0 && keys[i] != null && keys[i].getKey().equals(KV.getKey()) ) {
                keys[i].getValue().add(KV.getValue()); // Add original key to the list
            } else {
                // If the key doesn't exist, create a new list and insert the key-value pair
                List<Object> keyList = new ArrayList<>();
                keyList.add(KV.getValue()); // Add the original key to the list
                keys[i + 1] = new KeyValuePair<>(KV.getKey(), keyList);
                n++;
            }
        } else {
            while (i >= 0 && ((Comparable) KV.getKey()).compareTo(keys[i].getKey()) < 0) {
                i--;
            }
            i++;

            if (children[i].n == 2 * t - 1) {
                splitChild(i, children[i]);

                if (((Comparable) KV.getKey()).compareTo(keys[i].getKey()) > 0)
                    i++;
            }
            children[i].insertNonFull(KV);
        }
    }

    public List<Object> search(Object value) {
        int i = 0;

        // Traverse the node's keys to find the appropriate position for the value
        while (i < n && ((Comparable) value).compareTo(keys[i].getKey()) > 0) {
            i++;
        }

        // If the key matches the value, return the list of original keys (values in KeyValuePair)
        if (i < n && ((Comparable) value).compareTo(keys[i].getKey()) == 0) {
            return keys[i].getValue(); // Found the value, return the corresponding list of original keys
        }

        // If it's a leaf node and not found, return null
        if (leaf) {
            return new ArrayList<>(); // Value not found
        }

        // Otherwise, recursively search in the child node
        return children[i].search(value);
    }

    // Function to split the child node
    public void splitChild(int i, ReversedBTreeNode y) {
        ReversedBTreeNode z = new ReversedBTreeNode(y.t, y.leaf);
        z.n = t - 1;

        for (int j = 0; j < t - 1; j++)
            z.keys[j] = y.keys[j + t];

        if (!y.leaf) {
            for (int j = 0; j < t; j++)
                z.children[j] = y.children[j + t];
        }

        y.n = t - 1;

        for (int j = n; j >= i + 1; j--)
            children[j + 1] = children[j];

        children[i + 1] = z;

        for (int j = n - 1; j >= i; j--)
            keys[j + 1] = keys[j];

        keys[i] = y.keys[t - 1];
        n++;
    }

    // Function to print all keys and values (lists) in the subtree rooted with this node
    public void printInOrder() {
        int i;
        for (i = 0; i < n; i++) {
            if (!leaf)
                children[i].printInOrder();
            System.out.print(keys[i] + " ");
        }

        if (!leaf)
            children[i].printInOrder();
    }

    public List<Object> searchRange(Object value, String operator) {
        List<Object> result = new ArrayList<>();
        int i = 0;

        // Traverse through the keys and collect values based on the operator
        if (operator.equals(">") || operator.equals(">=")) {
            // Case for > or >=, search for values greater than or equal to the input value
            while (i < n && ((Comparable<Object>) keys[i].getKey()).compareTo(value) < 0) {
                i++; // Skip all keys smaller than the value
            }

            // Process the rest of the keys (keys[i] >= value)
            for (; i < n; i++) {
                if ((operator.equals(">") && ((Comparable<Object>) keys[i].getKey()).compareTo(value) > 0) ||
                    (operator.equals(">=") && ((Comparable<Object>) keys[i].getKey()).compareTo(value) >= 0)) {
                    result.addAll(keys[i].getValue()); // Add the keys that match the range
                }
                if (!leaf) {
                    result.addAll(children[i].searchRange(value, operator));
                }
            }

            // Check the rightmost child
            if (!leaf) {
                result.addAll(children[i].searchRange(value, operator));
            }
        } else if (operator.equals("<") || operator.equals("<=")) {
            // Case for < or <=, search for values less than or equal to the input value
            while (i < n && ((Comparable<Object>) keys[i].getKey()).compareTo(value) <= 0) {
                if ((operator.equals("<") && ((Comparable<Object>) keys[i].getKey()).compareTo(value) < 0) ||
                    (operator.equals("<=") && ((Comparable<Object>) keys[i].getKey()).compareTo(value) <= 0)) {
                    result.addAll(keys[i].getValue()); // Add the keys that match the range
                }
                if (!leaf) {
                    result.addAll(children[i].searchRange(value, operator));
                }
                i++;
            }

            // Check the last child node if necessary
            if (!leaf && i <= n) {
                result.addAll(children[i].searchRange(value, operator));
            }
        }

        return result;
    }
}

public class ReversedBTree {
    private ReversedBTreeNode root;
    private int t;

    public ReversedBTree(int t) {
        this.t = t;
        root = null;
    }

    // Insert key-value pair into the reversed B-Tree
    public void insert(KeyValuePair<Object, Object> KV) {
        if (root == null) {
            root = new ReversedBTreeNode(t, true);
            List<Object> keyList = new ArrayList<>();
            keyList.add(KV.getValue());
            root.keys[0] = new KeyValuePair<>(KV.getKey(), keyList);
            root.n = 1;
        } else {
            if (root.n == 2 * t - 1) {
                ReversedBTreeNode newRoot = new ReversedBTreeNode(t, false);
                newRoot.children[0] = root;
                newRoot.splitChild(0, root);

                int i = 0;
                if (((Comparable) newRoot.keys[0].getKey()).compareTo(KV.getKey()) < 0)
                    i++;

                newRoot.children[i].insertNonFull(KV);
                root = newRoot;
            } else {
                root.insertNonFull(KV);
            }
        }
    }

    // Print the entire reversed B-Tree
    public void printBTree() {
        if (root != null)
            root.printInOrder();
        System.out.println();
    }

    public List<Object> searchByValue(Object value) {
        if (root != null) {
            return root.search(value); // Start searching from the root node
        }
        return new ArrayList<>(); // If root is null, the tree is empty, so return null
    }

    public List<Object> searchByRange(Object value, String operator) {
        if (root != null) {
            return root.searchRange(value, operator); // Start searching from the root node
        }
        return new ArrayList<>(); // Return an empty list if the tree is empty
    }

    
}
