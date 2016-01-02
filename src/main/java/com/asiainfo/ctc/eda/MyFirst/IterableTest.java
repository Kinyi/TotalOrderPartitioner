package com.asiainfo.ctc.eda.MyFirst;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IterableTest {
	
	public static void main(String[] args) {
		/*Iterable<String> iter = new Iterable<String>() {
            public Iterator<String> iterator() {
                List<String> l = new ArrayList<String>();
                l.add("aa");
                l.add("bb");
                l.add("cc");
                return l.iterator();
            }
        };
        for(int count : new int[] {1, 2}){
            for (String item : iter) {
                System.out.println(item);
            }
            System.out.println("---------->> " + count + " END.");
        }*/
        
        
        
        ArrayList<String> list = new ArrayList<String>();
        
        list.add("kinyi");
        list.add("allen");
        list.add("bryant");
        
        /*for (String string : list) {
			System.out.println(string);
		}
        
        for (String string : list) {
			System.out.println(string);
		}*/
        
//        Iterator<String> iterator1 = list.iterator();
//        Iterator<String> iterator2 = list.iterator();
        
        /*while (iterator1.hasNext()) {
			String next1 = iterator1.next();
			System.out.println(next1);
		}
        
        while (iterator1.hasNext()) {
			String next2 = iterator1.next();
			System.out.println(next2);
		}*/
        
        int a;
        int b;
        a = b = 1;
        System.out.println(a);
        System.out.println(b);
	}
}