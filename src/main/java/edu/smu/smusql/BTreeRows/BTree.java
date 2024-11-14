// Java Program for Implementaion B-Tree
package edu.smu.smusql.BTreeRows;

class BTreeNode {
    // Variables Declared    
    KeyValuePair[] keys; // Array to store keys 


    int t; // Minimum degree (defines the range for number of keys)
    BTreeNode[] children; // Array to store child pointers
    int n; // Current number of keys
    boolean leaf; // True when node is leaf, else False

    public BTreeNode(int t, boolean leaf) {
        this.t = t;
        this.leaf = leaf;

        keys = new KeyValuePair[2 * t - 1];
        children = new BTreeNode[2 * t];
        n = 0;
    }

    // Search for a key and return the KeyValuePair object if found
    public KeyValuePair<Object, Object> getPair(Object key) {
        int i = 0;

        // Find the first key that is greater than or equal to the search key
        while (i < n && ((Comparable) keys[i].getKey()).compareTo(key) < 0) {
            i++;
        }

        // If the key is found, return the associated KeyValuePair object
        if (i < n && ((Comparable) keys[i].getKey()).compareTo(key) == 0) {
            return keys[i];  // Return the KeyValuePair object
        }

        // If the key is not found and this is a leaf node, return null
        if (leaf) {
            return null;
        }

        // Recursively search in the appropriate child node
        return children[i].getPair(key);
    }

    

    // Function to search for the given key in the subtree rooted with this node
    public Object search(Object key) {
        int i = 0;

        // Find the first key greater than or equal to the search key
        while (i < n && ((Comparable) key).compareTo(keys[i].getKey()) > 0) {
            i++;
        }

        // If the found key matches the search key, return the value
        if (i < n && ((Comparable) key).compareTo(keys[i].getKey()) == 0) {
            return keys[i].getValue();
        }

        // If this is a leaf node, the key is not present in the tree
        if (leaf) {
            return null;
        }

        // Otherwise, search in the appropriate child
        return children[i].search(key);
    }


    // Function to insert a new key
    // in subtree rooted with this node
    public void insertNonFull(KeyValuePair KV) {
        int i = n - 1;

        if (leaf) {
            while (i >= 0 && ((Comparable) KV.getKey()).compareTo(keys[i].getKey()) < 0) {
                keys[i + 1] = keys[i];
                i--;
            }
            keys[i + 1] = KV;
            n++;
        } else {
            while (i >= 0 && ((Comparable) KV.getKey()).compareTo(keys[i].getKey()) < 0)
                i--;
            i++;

            if (children[i].n == 2 * t - 1) {
                splitChild(i, children[i]);
                if (((Comparable) KV.getKey()).compareTo(keys[i].getKey()) > 0)
                    i++;
            }
            children[i].insertNonFull(KV);
        }
    }

    // Function to split the child node
    public void splitChild(int i, BTreeNode y) {
        BTreeNode z = new BTreeNode(y.t, y.leaf);
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

    // Function to print all keys in the
      // subtree rooted with this node
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

    // A utility function to remove the key from this node
    public void remove(Object key) {
        int idx = findKey(key);

        // If the key is found in this node
        
        if (idx < n && ((Comparable) keys[idx].getKey()).compareTo(key) == 0) {
            if (leaf) {
                removeFromLeaf(idx); // Key is in the leaf
            } else {
                removeFromNonLeaf(idx); // Key is in the internal node
            }
        } else {
            if (leaf) {
                System.out.println("The key " + key + " is not present in the tree");
                return;
            }

            boolean flag = (idx == n);

            if (children[idx].n < t) {
                fill(idx);
            }

            if (flag && idx > n) {
                children[idx - 1].remove(key);
            } else {
                children[idx].remove(key);
            }
        }
    }

    // Function to find the first index of the key in this node
    private int findKey(Object key) {
        int idx = 0;
        while (idx < n && ((Comparable) keys[idx].getKey()).compareTo(key) < 0) {
            idx++;
        }
        return idx;
    }

    // Function to remove the key at idx in a leaf node
    private void removeFromLeaf(int idx) {
        for (int i = idx + 1; i < n; ++i) {
            keys[i - 1] = keys[i];
        }
        n--;
    }

    // Function to remove the key at idx in an internal node
    private void removeFromNonLeaf(int idx) {
        Object key = keys[idx].getKey();

        if (children[idx].n >= t) {
            KeyValuePair<Object, Object> pred = getPredecessor(idx);
            keys[idx] = pred;
            children[idx].remove(pred.getKey());
        } else if (children[idx + 1].n >= t) {
            KeyValuePair<Object, Object> succ = getSuccessor(idx);
            keys[idx] = succ;
            children[idx + 1].remove(succ.getKey());
        } else {
            merge(idx);
            children[idx].remove(key);
        }
    }

    // Function to get the predecessor
    private KeyValuePair<Object, Object> getPredecessor(int idx) {
        BTreeNode cur = children[idx];
        while (!cur.leaf) {
            cur = cur.children[cur.n];
        }
        return cur.keys[cur.n - 1];
    }

    // Function to get the successor
    private KeyValuePair<Object, Object> getSuccessor(int idx) {
        BTreeNode cur = children[idx + 1];
        while (!cur.leaf) {
            cur = cur.children[0];
        }
        return cur.keys[0];
    }

    // A function to fill the child at idx
    private void fill(int idx) {
        if (idx != 0 && children[idx - 1].n >= t) {
            borrowFromPrev(idx);
        } else if (idx != n && children[idx + 1].n >= t) {
            borrowFromNext(idx);
        } else {
            if (idx != n) {
                merge(idx);
            } else {
                merge(idx - 1);
            }
        }
    }

    // Function to borrow a key from the previous sibling
    private void borrowFromPrev(int idx) {
        BTreeNode child = children[idx];
        BTreeNode sibling = children[idx - 1];

        for (int i = child.n - 1; i >= 0; --i) {
            child.keys[i + 1] = child.keys[i];
        }

        if (!child.leaf) {
            for (int i = child.n; i >= 0; --i) {
                child.children[i + 1] = child.children[i];
            }
        }

        child.keys[0] = keys[idx - 1];
        if (!leaf) {
            child.children[0] = sibling.children[sibling.n];
        }

        keys[idx - 1] = sibling.keys[sibling.n - 1];
        child.n += 1;
        sibling.n -= 1;
    }

    // Function to borrow a key from the next sibling
    private void borrowFromNext(int idx) {
        BTreeNode child = children[idx];
        BTreeNode sibling = children[idx + 1];

        child.keys[child.n] = keys[idx];
        if (!child.leaf) {
            child.children[child.n + 1] = sibling.children[0];
        }

        keys[idx] = sibling.keys[0];
        for (int i = 1; i < sibling.n; ++i) {
            sibling.keys[i - 1] = sibling.keys[i];
        }

        if (!sibling.leaf) {
            for (int i = 1; i <= sibling.n; ++i) {
                sibling.children[i - 1] = sibling.children[i];
            }
        }

        child.n += 1;
        sibling.n -= 1;
    }

    private void merge(int idx) {
        BTreeNode child = children[idx];
        BTreeNode sibling = children[idx + 1];
    
        // Pull down the key from the current node and place it at the end of the child node
        child.keys[t - 1] = keys[idx];
    
        // Move all the keys from the sibling to the child
        for (int i = 0; i < sibling.n; ++i) {
            child.keys[i + t] = sibling.keys[i];
        }
    
        // If the child is not a leaf, move the child pointers from the sibling to the child
        if (!child.leaf) {
            for (int i = 0; i <= sibling.n; ++i) {
                child.children[i + t] = sibling.children[i];
            }
        }
    
        // Shift all keys after idx in the current node one step to the left
        for (int i = idx + 1; i < n; ++i) {
            keys[i - 1] = keys[i];
        }
    
        // Shift the child pointers after idx+1 one step to the left
        for (int i = idx + 2; i <= n; ++i) {
            children[i - 1] = children[i];
        }
    
        // Update the number of keys in the child node
        child.n += sibling.n + 1;
    
        // Reduce the number of keys in the current node
        n--;
    }
    
}

public class BTree {
    // Pointer to root node
    private BTreeNode root;

    // Minimum degree
    private int t;

    public BTree(int t) {
        this.t = t;
        root = null;
    }

    // Function to search a key in this tree
    public Object search(Object key) {
        return (root == null) ? null : root.search(key);
    }

    // Function to insert a key into the B-tree
    public void insert(KeyValuePair KV) {
        if (root == null) {
            root = new BTreeNode(t, true);
            root.keys[0] = KV;
            root.n = 1;
        } else {
            if (root.n == 2 * t - 1) {
                BTreeNode newRoot = new BTreeNode(t, false);
                newRoot.children[0] = root;
                newRoot.splitChild(0, root);

                int i = 0;

                if (((Comparable) KV.getKey()).compareTo(newRoot.keys[i].getKey()) > 0)
                    i++;

                newRoot.children[i].insertNonFull(KV);
                root = newRoot;
            } else {
                root.insertNonFull(KV);
            }
        }
    }

    // Function to delete a key in the B-tree
    public void delete(Object key) {
        if (root == null) {
            System.out.println("The tree is empty");
            return;
        }

        // Call the remove function
        root.remove(key);

        // If the root node has 0 keys, make its first child the new root (if it exists)
        if (root.n == 0) {
            if (root.leaf) {
                root = null; // If the root is empty and it's a leaf, the tree becomes empty
            } else {
                root = root.children[0]; // Make the first child the new root
            }
        }
    }

    // Search for a key and return its associated KeyValuePair object
    public KeyValuePair<Object, Object> getPair(Object key) {
        if (root == null) {
            return null; // If the root is null, the tree is empty
        }
        return root.getPair(key); // Call the search method in BTreeNode
    }

    // Update the value associated with a given key
    public boolean update(Object key, Object newValue) {
        KeyValuePair<Object, Object> kv = getPair(key); // Search for the key

        if (kv != null) {
            kv.setValue(newValue); // Update the value
            // System.out.println("Key " + key + " updated to new value: " + newValue);
            return true; // Return true to indicate success
        } else {
            // System.out.println("Key " + key + " not found in the B-Tree.");
            return false; // Return false if the key is not found
        }
    }

    // Function to print the entire B-tree
    public void printBTree() {
        if (root != null)
            root.printInOrder();

        System.out.println();
    }



    public static void main(String[] args) {
        // Create a B-tree with minimum degree 3
        BTree bTree = new BTree(3);
        bTree.insert(new KeyValuePair<Integer, String>(1, "test"));
        bTree.insert(new KeyValuePair<Integer, String>(2, "test2"));
        bTree.insert(new KeyValuePair<Integer, String>(3, "test3"));
        bTree.insert(new KeyValuePair<Integer, String>(4, "test4"));
        bTree.insert(new KeyValuePair<Integer, String>(5, "test5"));
        bTree.insert(new KeyValuePair<Integer, String>(6, "test6"));
        bTree.insert(new KeyValuePair<Integer, String>(7, "test7"));
        bTree.insert(new KeyValuePair<Integer, String>(8, "test8"));


        System.out.print("B-tree : ");
        bTree.printBTree();


        bTree.delete(6);
        bTree.printBTree();

        // if (foundNode != null)
        //     System.out.println("Key " + searchKey + " found in the B-tree.");
        // else
        //     System.out.println("Key " + searchKey + " not found in the B-tree.");
    }
}

