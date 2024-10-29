package edu.smu.smusql;

import edu.smu.smusql.ArrayList.ALEngine;
import edu.smu.smusql.BTree.BTreeEngine;
import edu.smu.smusql.HashMap.HashMapEngine;
import edu.smu.smusql.MapHeapMap.MapHeapMapEngine;
import edu.smu.smusql.Treemap.TMEngine;

public class EngineFactory {

    public static Engine getEngine(String engineType) {
        switch (engineType.toUpperCase()) {
            case "BTREE":
                return new BTreeEngine();

            case "TREEMAP":
                return new TMEngine();

            case "ARRAYLIST":
                return new ALEngine();

            case "HASHMAP":
                return new HashMapEngine();

            case "MAPHEAPMAP":
                return new MapHeapMapEngine();

            case "DEFAULT":
                return new DefaultEngine();

            default:
                throw new IllegalArgumentException("Unsupported database engine: " + engineType);
        }
    }

}
