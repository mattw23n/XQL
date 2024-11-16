package edu.smu.smusql.MapHeapMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class MinHeap<K extends Comparable<K>, V> {

    // Entry class to store key-value pairs
    static class Entry<K extends Comparable<K>, V> {
        K key;
        V value;

        public Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public void setKey(K newKey) {
            key = newKey;
        }

        public V getValue() {
            return value;
        }

        public void setValue(V newValue) {
            value = newValue;
        }

        @Override
        public String toString() {
            return "(" + key + ", " + value + ")";
        }
    }

    private List<Entry<K, V>> heap;

    // Constructor to initialize the heap
    public MinHeap() {
        heap = new ArrayList<>();
    }

    // Get parent, left child, and right child indices
    private int getParentIndex(int i) { return (i - 1) / 2; }
    private int getLeftChildIndex(int i) { return 2 * i + 1; }
    private int getRightChildIndex(int i) { return 2 * i + 2; }

    // Check if a node has parent, left child, or right child
    private boolean hasParent(int i) { return getParentIndex(i) >= 0; }
    private boolean hasLeftChild(int i) { return getLeftChildIndex(i) < heap.size(); }
    private boolean hasRightChild(int i) { return getRightChildIndex(i) < heap.size(); }

    // Get parent, left child, and right child values
    private Entry<K, V> parent(int i) { return heap.get(getParentIndex(i)); }
    private Entry<K, V> leftChild(int i) { return heap.get(getLeftChildIndex(i)); }
    private Entry<K, V> rightChild(int i) { return heap.get(getRightChildIndex(i)); }

    // Swap two elements in the heap
    private void swap(int i, int j) {
        Entry<K, V> temp = heap.get(i);
        heap.set(i, heap.get(j));
        heap.set(j, temp);
    }

    // Insert a new entry into the heap
    public void insert(K key, V value) {
        Entry<K, V> newEntry = new Entry<>(key, value);  // Create a new entry
        heap.add(newEntry);  // Add the entry to the end
        heapifyUp(heap.size() - 1);  // Restore the heap property by moving the entry up
    }

    // Get the minimum element (root of the heap)
    public Entry<K, V> peekMin() {
        if (heap.isEmpty()) throw new IllegalStateException("Heap is empty");
        return heap.get(0);  // The root entry is the smallest in a min-heap
    }

    // Remove and return the minimum element (root of the heap)
    public Entry<K, V> removeMin() {
        if (heap.isEmpty()) throw new IllegalStateException("Heap is empty");

        Entry<K, V> min = heap.get(0);
        // Replace the root with the last element and remove the last element
        heap.set(0, heap.get(heap.size() - 1));
        heap.remove(heap.size() - 1);

        heapifyDown(0);  // Restore the heap property by moving the new root down
        return min;
    }

    // Heapify up to maintain heap property after insertion
    private void heapifyUp(int index) {
        while (hasParent(index) && parent(index).getKey().compareTo(heap.get(index).getKey()) > 0) {
            swap(getParentIndex(index), index);  // Swap with the parent if necessary
            index = getParentIndex(index);  // Move up to the parent's index
        }
    }

    // Heapify down to maintain heap property after deletion
    private void heapifyDown(int index) {
        while (hasLeftChild(index)) {
            int smallerChildIndex = getLeftChildIndex(index);

            // Check if right child exists and is smaller than left child
            if (hasRightChild(index) && rightChild(index).getKey().compareTo(leftChild(index).getKey()) < 0) {
                smallerChildIndex = getRightChildIndex(index);
            }

            // If the current element is smaller than both children, stop
            if (heap.get(index).getKey().compareTo(heap.get(smallerChildIndex).getKey()) <= 0) {
                break;
            }

            // Otherwise, swap with the smaller child and continue heapifying down
            swap(index, smallerChildIndex);
            index = smallerChildIndex;
        }
    }

    // Method to delete an entry from the heap based on the key
    public void deleteByID(K key) {
        int indexToRemove = -1;

        // Step 1: Find the index of the entry with the given key
        for (int i = 0; i < heap.size(); i++) {
            if (heap.get(i).getKey().equals(key)) {
                indexToRemove = i;
                break;
            }
        }

        // Step 2: If the key is not found, return
        if (indexToRemove == -1) {
            System.out.println("Key not found in the heap.");
            return;
        }

        // Step 3: Swap the element to be deleted with the last element
        swap(indexToRemove, heap.size() - 1);

        // Step 4: Remove the last element (the one to be deleted)
        heap.remove(heap.size() - 1);

        // Step 5: Heapify to restore the heap property
        if (indexToRemove < heap.size()) {
            if (hasParent(indexToRemove) && heap.get(indexToRemove).getKey().compareTo(parent(indexToRemove).getKey()) < 0) {
                heapifyUp(indexToRemove);  // If the entry is smaller than its parent, heapify up
            } else {
                heapifyDown(indexToRemove);  // Otherwise, heapify down
            }
        }
    }

    // Display the heap
    public void printHeap() {
        System.out.println(heap);
    }

    //Traverse heap & add to set 
    public Set<Entry<K, V> > getRowsInTable() { //Assume its smusql.get(tableName)
        Set<Entry<K, V> > rows = new HashSet<>();
        for (Entry<K, V> entry : heap) {
            rows.add(entry);
        }
        return rows;
    }

    //Traverse heap & add to set 
    public Set<Entry<K, V> > getRowsInTable(List<String> target) { //Assume its smusql.get(tableName)
        Set<Entry<K, V> > rows = new HashSet<>();
        for (Entry<K, V> entry : heap) {
            for (String t : target) {
                V columns = entry.getValue();
                
            }
        }
        return rows;
    }

    // Get value by key method
    public V getValueByKey(K key) {
        for (Entry<K, V> entry : heap) {
            if (entry.getKey().equals(key)) {
                return entry.getValue(); // Return value if key matches
            }
        }
        return null; // Return null if the key is not found
    }

}
