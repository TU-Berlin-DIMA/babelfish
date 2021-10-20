package de.tub.dima.babelfish.storage;

import de.tub.dima.babelfish.storage.layout.*;

import java.util.*;

/**
 * Singleton catalog to register and access memory buffers of relations.
 */
public class Catalog {

    private static Catalog instance;

    private HashMap<String, PhysicalLayout> relationmap = new HashMap<>();
    private HashMap<String, Buffer> bufferMap = new HashMap<>();
    private HashMap<String, Object> arrowReader = new HashMap<>();


    private Catalog() {
    }

    public static Catalog getInstance() {
        if (instance == null)
            instance = new Catalog();
        return instance;
    }

    public void registerLayout(String name, PhysicalLayout physicalLayout) {
        relationmap.put(name, physicalLayout);
    }
    public void registerArrowLayout(String name, Object physicalLayout) {
        arrowReader.put(name, physicalLayout);
    }

    public void registerBuffer(String name, Buffer buffer) {
        bufferMap.put(name, buffer);
    }

    public PhysicalLayout getLayout(String name) {
        return relationmap.get(name);
    }


    public Buffer getBuffer(String layout) {
        return bufferMap.get(layout);
    }
    public Object getArrow(String layout) {
        return arrowReader.get(layout);
    }
}
