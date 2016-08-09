package com.alg;



/**
 * Created by konglu on 2016/8/4.
 */
public class Test {
    public static void main(String... args) throws InterruptedException {
        final String a=new String("A");
        final String b=new String("B");
        final String c=new String("C");

        Thread t1=new Thread(new Runnable() {
            @Override
            public void run() {
                int count=10;
                while (count>0){
                    synchronized(c){
                        synchronized (a){
                            System.out.print(a.toString());
                            count--;
                            a.notifyAll();
                        }
                        try {
                            c.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                }
            }
        });
        Thread t2=new Thread(new Runnable() {
            @Override
            public void run() {
                int count=10;
                while (count>0){
                    synchronized(a){
                        synchronized (b){
                            System.out.print(b.toString());
                            count--;
                            b.notifyAll();
                        }
                        try {
                            a.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        Thread t3=new Thread(new Runnable() {
            @Override
            public void run() {
                int count=10;
                while (count>0){
                    synchronized(b){
                        synchronized (c){
                            System.out.print(c.toString());
                            count--;
                            c.notifyAll();
                        }
                        try {
                            b.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });


        t1.start();
        Thread.sleep(100);
        t2.start();
        Thread.sleep(100);
        t3.start();
        Thread.sleep(100);
    }
}
