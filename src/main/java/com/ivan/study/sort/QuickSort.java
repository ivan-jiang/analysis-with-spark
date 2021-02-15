package com.ivan.study.sort;

import java.util.concurrent.atomic.AtomicInteger;

public class QuickSort {
    private AtomicInteger swapCounter = new AtomicInteger(0);

    public static void main(String[] args) {
        Double[] data = new Double[]{5.0, 4.0, 2.0, 3.9, 7.9, 6.0, 5.0, 7.9, 8.0, 9.0};
        new QuickSort().start(data);
        for (int i = 0; i < data.length; i++) {
            System.err.println(data[i]);
        }
    }

    private void start(Comparable[] data) {
        if (data == null || data.length <= 1) return;
        quickSort(data, 0, data.length - 1);
    }

    private void quickSort(Comparable[] data, int start, int end) {
        // 递归退出
        if (end <= start) return;

        Comparable root = data[start];
        int i = start;
        int j = end;
        while (i < j) {
            while (i < j && data[j].compareTo(root) >= 0) {
                j--;
            }
            if (i < j) swap(data, i, j);


            while (i < j && data[i].compareTo(root) < 0) {
                i++;
            }
            if (i < j) swap(data, i, j);
        }

        data[j] = root;

        quickSort(data, start, i);
        quickSort(data, i + 1, end);
    }

    private void swap(Comparable[] data, int i, int j) {
        System.err.println("count " + swapCounter.incrementAndGet() + "," + i + "," + j);
        Comparable tmp = data[i];
        data[i] = data[j];
        data[j] = tmp;
    }
}
