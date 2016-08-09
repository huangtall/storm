package com.alg;

/**
 * Created by konglu on 2016/8/4.
 */
public class TestThread {
    public static void main(String... args) throws InterruptedException {
        final String obj=new String("t");
        Thread t1=new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (obj){
                    try {
                        obj.wait();
                        System.out.print("A");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        });
        Thread t2=new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (obj){
                    try {
                        obj.wait();
                        System.out.print("B");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        });
        Thread t3=new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (obj){
                    System.out.print("C");
                    obj.notifyAll();
                }
            }
        });

        t1.start();
        Thread.sleep(500);
        t2.start();
        Thread.sleep(500);
        t3.start();

    }
}
