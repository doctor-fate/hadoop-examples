package ru.bmstu.hadoop.spark;

import scala.Serializable;

public class Statistics implements Serializable {
    final static Statistics ZERO = new Statistics();

    private int delayed;
    private int all;
    private float max;

    private Statistics() { }

    private Statistics(int delayed, int all, float max) {
        this.delayed = delayed;
        this.all = all;
        this.max = max;
    }

    Statistics add(Flight flight) {
        int all = this.all + 1;
        int delayed = this.delayed;
        float max = this.max;
        if (flight.isCancelledOrDelayed()) {
            delayed++;
            max = Math.max(max, flight.getDelay());
        }
        return new Statistics(delayed, all, max);
    }

    Statistics merge(Statistics s) {
        int delayed = this.delayed + s.delayed;
        int all = this.all + s.all;
        float max = this.max + s.max;
        return new Statistics(delayed, all, max);
    }

    @Override
    public String toString() {
        return String.format("(%.2f%%,%.2f)", ((float) delayed / all) * 100, max);
    }
}
