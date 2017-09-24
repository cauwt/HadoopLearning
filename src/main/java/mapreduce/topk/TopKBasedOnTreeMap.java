package mapreduce.topk;

import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

public class TopKBasedOnTreeMap {
	
	public static void main(String[] args) {
	    testTreeMap();
	}

	public static void testSimpleTM() {
        TreeMap<Long, Long> tree = new TreeMap<>();
        tree.put(1111111L, 1111111L);
        tree.put(1333333L, 1333333L);
        tree.put(1222222L, 1222222L);
        tree.put(1555555L, 1555555L);
        tree.put(1444444L, 1444444L);
        tree.put(1111111L, 1111111L);

        for (Entry<Long, Long> entry : tree.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        System.out.println(tree.firstEntry().getValue());
        System.out.println(tree.lastEntry().getValue());
        System.out.println(tree.navigableKeySet());
        System.out.println(tree.descendingKeySet());
    }

	public static void testTreeMap() {
	    TreeMap<MyScore, String> tree = new TreeMap<>(((o1, o2) -> o2.compareTo(o1)));
        MyScore score1 = new MyScore(90);
        MyScore score2 = new MyScore(100);
        MyScore score3 = new MyScore(100);
        MyScore score4 = new MyScore(98);
        MyScore score5 = new MyScore(95);

        System.out.println(score1);
        System.out.println(score2);
        System.out.println(score3);
        System.out.println(score4);
        System.out.println(score5);

        tree.put(score1, "Jangz");
	    tree.put(score2, "Daniel");
	    tree.put(score3, "Marvin");
	    System.out.println("current: " + tree.size());
        if (tree.size() > 3) {
            tree.remove(tree.lastKey());
        }
	    tree.put(score4, "David");
        System.out.println("current: " + tree.size() + " key: " + tree.lastKey());
	    if (tree.size() > 3) {
	        Object k = tree.remove(tree.lastKey());
            System.out.println("remove: " + k);
        }
	    tree.put(score5, "Kevin");
        System.out.println("current: " + tree.size() + " key: " + tree.lastKey());
        if (tree.size() > 3) {
            Object k = tree.remove(tree.lastKey());
            System.out.println("remove: " + k);
        }

        Stream.of(tree.entrySet().stream()).flatMap(Function.identity()).forEach(t -> {
            System.out.println(t.getKey() + " : " + t.getValue());
        });
    }
}
