WAP TO FIND MAX VALUE?

import java.util.Scanner;

public class arrayprogram {
	public static void main(String args[])
	{
		Scanner s=new Scanner(System.in);
		int ar[]=new int[5];
		
		System.out.println("enter value");//3,2,1,4,5
		int temp=0;
		//int arr;
		
		for(int x=0;x<5;x++){//accepting values from user
			ar[x]=s.nextInt();
		}
		
		for(int i=0;i<5;i++)//
		{
			for(int j=i+1;j<5;j++)
			{
				if(ar[i]>ar[j])//swap if 1st element is greater than 2nd one
				{
					temp=ar[i];
					ar[i]=ar[j];
					ar[j]=temp;
				}
			}System.out.println(ar[i]);
		}
		
		
		
	}

}


WAP TO FIND AN AVERAGE?


import java.util.Scanner;

public class average {
	public static void main(String args[])
	{
		  int n, add = 0;
	        float average;
	        Scanner s = new Scanner(System.in);
	        System.out.print("Enter no. of elements you want in array:");
	        n = s.nextInt();
	        int a[] = new int[n];
	        System.out.println("Enter all the elements:");
	        for(int i = 0; i < n ; i++)
	        {
	            a[i] = s.nextInt();
	            add =add+a[i];
	        }
	        System.out.println("Sum:"+add);
	        average = add / n;
	        System.out.println("Average:"+average);
	}

}

WAP TO FIND EMPLOYEE BONOUS ACCORDING TO USER REQUIREMENTS?
import java.util.Scanner;

public class Employe {

		 private String name;
		    private static double empSal;

		    public Employe(String e, double s){
		        name = e;
		        empSal = s;
		    }

		    public String getname(){
		        return name;
		    }
		    public  double getSalary(){
		        return empSal;

		    }

		    public static void main(String[] args) {

		        Employe[] array = new Employe[10];
		        Scanner sc = new Scanner(System.in);        
		        for(int i = 0; i < 10; i++) {
		            System.out.printf("Please enter your employee name:");
		            String e = sc.next();
		            System.out.printf("Please enter your salary:");
		            double s = sc.nextDouble();
		            array[i] = new Employe(e, s);
		            if(empSal==10000)
		            {
		            	System.out.println("bonus is "+ 10);
		            }
		            if(empSal==50000)
		            {
		            	System.out.println("bonus is "+ 30);
		            }
		            if(empSal==100000)
		            {
		            	System.out.println("bonus is "+ 50);
		            }
		        }
		    }
		}


WAP TO FIND TOP SCORER AND NAME?
import java.util.*;
public class highestscore {
	public static void main(String args[])
	{
	Scanner s=new Scanner(System.in);
	
	int array[]=new int[5];
	String ar[]=new String[5];
	int i;
	for(i=0;i<5;i++)
	{
		System.out.print("enter student name");
		ar[i]=s.next();
	System.out.print("enter student score");
	array[i]=s.nextInt();
	}
	int max=0;
	String top="";
	for(int j=0;j<5;j++)
	{
		if(array[j]>max)
		{
	     max=array[j];
	     top = ar[j];
		}
	
	}
	System.out.println("max score student is:"+max+" name of the student is "+top);
	}
}

WAP TO FIND STUDENT MUST BE ELIGABLE WHO'S AGE IS ABOVE 18?
import java.util.Scanner;

public class student1 {
	
	public static void main(String args[]){
		
		Scanner s=new Scanner(System.in);
		int age = 0;
		String name;
		int r=0;
		
		for(int i=0;i<3;i++)
		{
	
			System.out.println("enter student name");
			System.out.println("enter student age");
			age=s.nextInt();	
			name=s.next();
			
			if(age>18)
			{
				
				System.out.println("student is elgible");
				r=r+1;
			}
			else
			{
				System.out.println("age must be >18");
			}
		}
		System.out.println("no eligible "+r);
	}

}
