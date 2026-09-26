package com.una.kafka.connect.solr;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads the given packages/classes afresh (child-first) from the real build output, the way a
 * Connect worker's plugin loader isolates a connector. Static initialisers therefore run again,
 * against the resources this loader serves. Classes keep their code source, so JaCoCo still
 * attributes their coverage to the same classes.
 */
final class ChildFirstClassLoader extends URLClassLoader {

    private final List<String> childFirstPrefixes;
    private final Map<String, byte[]> resourceOverrides = new HashMap<>();

    ChildFirstClassLoader(URL[] urls, String... childFirstPrefixes) {
        super(urls, ChildFirstClassLoader.class.getClassLoader());
        this.childFirstPrefixes = Arrays.asList(childFirstPrefixes);
    }

    /** Serve {@code content} for resource {@code name}; {@code null} makes the resource absent. */
    ChildFirstClassLoader withResource(String name, byte[] content) {
        resourceOverrides.put(name, content);
        return this;
    }

    static URL codeSourceOf(Class<?> type) {
        return type.getProtectionDomain().getCodeSource().getLocation();
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            if (childFirstPrefixes.stream().noneMatch(name::startsWith)) {
                return super.loadClass(name, resolve);
            }
            Class<?> c = findLoadedClass(name);
            if (c == null) {
                try {
                    c = findClass(name);
                } catch (ClassNotFoundException notHere) {
                    c = super.loadClass(name, false);
                }
            }
            if (resolve) resolveClass(c);
            return c;
        }
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        if (resourceOverrides.containsKey(name)) {
            byte[] content = resourceOverrides.get(name);
            return content == null ? null : new ByteArrayInputStream(content);
        }
        return super.getResourceAsStream(name);
    }
}
