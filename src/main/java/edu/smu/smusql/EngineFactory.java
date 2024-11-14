package edu.smu.smusql;

import edu.smu.smusql.ArrayList.ALEngine;
import edu.smu.smusql.BTree.BTreeEngine;
import edu.smu.smusql.BTreeRows.BTreeRowEngine;
import edu.smu.smusql.HashMap.HashMapEngine;
import edu.smu.smusql.MapHeapMap.*;
import edu.smu.smusql.Treemap.TMEngine;
import edu.smu.smusql.CircularLinkedList.CLLEngine;

public class EngineFactory {

    public static Engine getEngine(String engineType) {
        switch (engineType.toUpperCase()) {
            case "BTREECOLS":
                return new BTreeEngine();

            case "BTREEROWS":
                return new BTreeRowEngine();

            case "TREEMAP":
                return new TMEngine();

            case "ARRAYLIST":
                return new ALEngine();

            case "HASHMAP":
                return new HashMapEngine();

            case "MAPHEAPMAP":
                return new HeapEngine();

            case "CIRCULARLINKEDLIST":
                return new CLLEngine();


            default:
                throw new IllegalArgumentException("Unsupported database engine: " + engineType);
        }
    }

}
