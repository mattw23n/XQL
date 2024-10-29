package edu.smu.smusql.ArrayList;

import java.util.*;

public class Row {
    private List<Object> data;

    public Row(List<Object> data) {
        this.data = data;
    }

    public List<Object> getData() {
        return data;
    }

    public Object getColumn(int index) {
        return data.get(index);
    }

    public void updateColumn(int index, Object value) {
        data.set(index, value);
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
