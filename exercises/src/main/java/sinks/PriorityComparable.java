package sinks;

public interface PriorityComparable extends Comparable<PriorityComparable> {
    int compareTo(PriorityComparable o);

    int priority();
}
