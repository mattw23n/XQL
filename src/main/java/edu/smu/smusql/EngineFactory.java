package edu.smu.smusql;

import edu.smu.smusql.BTree.BTreeEngine;

public class EngineFactory {

    public static Engine getEngine(String engineType){
        switch (engineType.toUpperCase()) {
            case "BTREE":
                return new BTreeEngine();
            
            case "DEFAULT":
                return new DefaultEngine();

            default:
                throw new IllegalArgumentException("Unsupported database engine: " + engineType);
        }
    }
    
}
