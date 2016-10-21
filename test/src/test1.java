import java.io.*;
import java.lang.*;
import java.util.*;
import java.net.*;

public class test1 {
public void main (String[] args) throws Exception{
	ArrayList<String> a = new ArrayList<String>();
	ArrayList<String> b = new ArrayList<String>();
	a.add("a");
	a.add("b");
	b.add("b");
	b.add("c");
	
	String removed = "";
	String added = "";
	for (String s1:a)
	{
		if(!b.contains(s1))
		{
			removed = removed + s1 + ",";
		}					
	}
	
	for (String s2:b)
	{
		if(!a.contains(s2))
		{
			added = added + s2 + ",";
		}
	}
	
	System.out.println("removed sensors: " + removed);
	System.out.println("added sensors: " + added);
	System.out.println();
}
}