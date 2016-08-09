package com.alg;

/**
 * Created by konglu on 2016/8/4.
 */
public class MulitPrint {
    public static void main(String... args) throws InterruptedException {
        final String obj=new String();
        boolean flag = false;
        Thread t1=new Thread(new Runnable() {
            @Override
            public void run() {
                for(int i=0;i<10;i++){
                    synchronized (obj){
                        System.out.print("t1");
                        obj.notifyAll();
                        try {
                            obj.wait();
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
                for(int i=0;i<10;i++){
                    synchronized (obj){
                        System.out.print("t2");
                        obj.notifyAll();
                        try {
                            obj.wait();
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
        Thread.sleep(5000);
        synchronized (obj){
            obj.notifyAll();
        }
    }
}
