package org.mapreduce;

import com.sun.xml.internal.bind.v2.TODO;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MapReduce {

    public static void main(String[] args) {

        Map<String, String> input = new HashMap<String, String>();
        for (String filename : args) {
            File file = new File("./" + filename);
            try {
                FileInputStream fileInputStream = new FileInputStream(file);
                byte[] contents = new byte[(int) file.length()];
                for (byte b : contents)
                    b = (byte) fileInputStream.read();
                fileInputStream.close();
                input.put(filename, new String(contents, "UTF-8"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

        // MAP:
        final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
        final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
            @Override
            public synchronized void mapDone(String file, List<MappedItem> results) {
                mappedItems.addAll(results);
            }
        };

        //TODO this way number of threads has to be fourth argument; should be configurable
        ExecutorService executorService = Executors.newFixedThreadPool(Integer.parseInt(args[3]));
        for (int i=0; i<Integer.parseInt(args[3]); i++){
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    //TODO figure out what to put in here
                }
            });
            executorService.execute(t);
        }
        executorService.shutdown();
        while (!executorService.isTerminated()){}
        System.out.println("All threads finished");

        /*List<Thread> mapCluster = new ArrayList<Thread>(input.size());
        for (Map.Entry<String, String> entry : input.entrySet()) {
            final String file = entry.getKey();
            final String contents = entry.getValue();

            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    map(file, contents, mapCallback);
                }
            });
            mapCluster.add(t);
            t.start();
        }

        // wait for mapping phase to be over:
        for(Thread t : mapCluster) {
            try {
                t.join();
            } catch(InterruptedException e) {
                throw new RuntimeException(e);
            }
        }*/

        // GROUP:
        Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
        for (MappedItem item : mappedItems) {
            String word = item.getWord();
            String file = item.getFile();
            List<String> list = groupedItems.get(word);
            if (list == null) {
                list = new LinkedList<String>();
                groupedItems.put(word, list);
            }
            list.add(file);
        }

        // REDUCE:
        final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
            @Override
            public synchronized void reduceDone(String k, Map<String, Integer> v) {
                output.put(k, v);
            }
        };

        List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());
        for (Map.Entry<String, List<String>> entry : groupedItems.entrySet()) {
            final String word = entry.getKey();
            final List<String> list = entry.getValue();

            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    reduce(word, list, reduceCallback);
                }
            });
            reduceCluster.add(t);
            t.start();
        }

        // wait for reducing phase to be over:
        for(Thread t : reduceCluster) {
            try {
                t.join();
            } catch(InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        System.out.println(output);
    }

    public interface MapCallback<E, V> {

        void mapDone(E key, List<V> values);
    }

    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for(String word: words) {
            results.add(new MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }

    public interface ReduceCallback<E, K, V> {

        void reduceDone(E e, Map<K,V> results);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }
}