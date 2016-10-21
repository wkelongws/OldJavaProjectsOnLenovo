package test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class yawtutor {

	public static void main(String[] args) throws FileNotFoundException, MalformedURLException {
		// TODO Auto-generated method stub

		int x = 3;
		int y = (int) Math.pow(x, 2);
		System.out.println(y);
		
		
		// arrays
		int[][][] X = new int[2][2][2];
		X[0][0][0] = 9;
		System.out.println(X);
		
		List<String> Y = new ArrayList<String>();
		Y.add("Mo");Y.add("Yu");Y.add("Shuo");
		System.out.println(Y);

		// loops 
		for (int i=0;i<10;i++){}
		
		// read txt file from local
		String path = "C:/Users/Shuo/Desktop/New Text Document.txt";
		FileReader fr = new FileReader (path);
		BufferedReader b = new BufferedReader(fr);
		int lines=7;
		System.out.println(b);
		
		// download data from webpage
		URL link = new URL("");
		
	}

}
