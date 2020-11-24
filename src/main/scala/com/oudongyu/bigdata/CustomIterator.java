package com.oudongyu.bigdata;


import java.io.Serializable;
import java.util.Iterator;

public class CustomIterator implements Iterator<Integer>, Serializable {
    private Integer i = 0;

    @Override
    public boolean hasNext() {
        return i < 100;
    }

    @Override
    public Integer next() {
        i++;
        return i;
    }
}
