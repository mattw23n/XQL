package edu.smu.smusql.ArrayList;

@FunctionalInterface
public interface Condition {
    boolean test(Row row);

    // Helper methods to combine conditions using AND or OR
    default Condition and(Condition other) {
        return row -> this.test(row) && other.test(row);
    }

    default Condition or(Condition other) {
        return row -> this.test(row) || other.test(row);
    }
}
