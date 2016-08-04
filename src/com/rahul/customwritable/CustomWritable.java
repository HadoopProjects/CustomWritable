package com.rahul.customwritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomWritable implements WritableComparable {

	private Text firstName;
	private Text lastName;

	public CustomWritable() {
	}

	public CustomWritable(String firstName, String lastName) {
		set(new Text(firstName), new Text(lastName));
	}

	public CustomWritable(Text firstName, Text lastName) {
		set(firstName, lastName);
	}

	public void set(Text firstName, Text lastName) {
		this.firstName = firstName;
		this.lastName = lastName;
	}

	public Text getFirstName() {
		return firstName;
	}

	public Text getLastName() {
		return lastName;
	}

	public void readFields(DataInput dataIn) throws IOException {
		firstName.readFields(dataIn);
		lastName.readFields(dataIn);
	}

	public void write(DataOutput dataOut) throws IOException {
		firstName.write(dataOut);
		lastName.write(dataOut);
	}

	public int compareTo(Object o) {
		CustomWritable obj = (CustomWritable) o;
		int cmp = firstName.compareTo(obj.firstName);
		if (cmp != 0) {
			return cmp;
		}
		return lastName.compareTo(obj.lastName);
	}
	
	public String toString(){
		return firstName.toString() + " " + lastName.toString();
	}

	public int hashCode() {
		return firstName.hashCode() * 163 + lastName.hashCode();
	}
	
}
