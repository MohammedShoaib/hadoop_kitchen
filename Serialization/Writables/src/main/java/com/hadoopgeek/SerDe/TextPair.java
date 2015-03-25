package com.hadoopgeek.SerDe;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;

public class TextPair implements WritableComparable<TextPair> {
	Text first;
	Text second;

	public TextPair() {

		set(new Text(), new Text());
	}

	public TextPair(Text first, Text second) {

		set(first, second);
	}

	public TextPair(String first, String second) {

		set(first, second);
	}

	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}

	public void set(String first, String second) {
		this.first = new Text(first);
		this.second = new Text(second);
	}

	public Text getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	public void write(DataOutput out) throws IOException {

		first.write(out);
		second.write(out);

	}

	public void readFields(DataInput in) throws IOException {

		first.readFields(in);
		second.readFields(in);

	}

	public int compareTo(TextPair o) {
		int cmp = first.compareTo(o.first);
		if (cmp != 0)
			return cmp;
		return second.compareTo(o.second);
	}

	@Override
	public String toString() {

		return first.toString() + "\t" + second.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TextPair) {
			TextPair tp = (TextPair) obj;
			return (this.first.equals(tp.first) && this.second
					.equals(tp.second));

		}
		return false;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return this.first.hashCode() * 163 + this.second.hashCode();
	}
	
	// register the comparator in the static initializer. Static block gets called when class gets loaded. 
	static
	{
		WritableComparator.define(TextPair.class, new TPComparator());
	}


}

/**
 * 
 * @author jithesh
 * Text in its binary representation contains a variablelength integer  which contains the length of actual bytes of text that follows. 
 * decodeVIntSize(b1[s1]) : This determines how many bytes represents variablelength integer
 * readVInt(b1, 0) : It actually reads the content of variable length which is actual bytes representing the bytes of stream following
 */

class TPComparator extends WritableComparator
{

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
	{
		System.out.println(StringUtils.byteToHexString(b1)); 
		System.out.println(StringUtils.byteToHexString(b2));
    
		try {
			int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, 0);
			int firstL2 = WritableUtils.decodeVIntSize(b2[s1]) + readVInt(b2, 0);
			RawComparator<Text> textComp =  WritableComparator.get(Text.class);
		    int comp =textComp.compare(b1,  s1, firstL1, b2, s2, firstL2);
		    if(comp !=0) return comp;
		    
		    comp = textComp.compare(b1, s1+ firstL1, l1 - firstL1, b2, s2+firstL2, l2 - firstL2);
		    
		    return comp;
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	    
	    
		return 0;
	}
	
}


