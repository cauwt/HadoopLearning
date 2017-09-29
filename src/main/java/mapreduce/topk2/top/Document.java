package mapreduce.topk2.top;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Document implements WritableComparable<Document> {

	private String name;

	private Integer age;

	private String gender;

	private Integer score;

	public Document() {
	}

	public Document(String name, Integer age, String gender, Integer score) {
		this.name = name;
		this.age = age;
		this.gender = gender;
		this.score = score;
	}
	
	public void set(String name, Integer age, String gender, Integer score) {
		this.name = name;
		this.age = age;
		this.gender = gender;
		this.score = score;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(age);
		out.writeUTF(gender);
		out.writeInt(score);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.age = in.readInt();
		this.gender = in.readUTF();
		this.score = in.readInt();
	}

	@Override
	public int compareTo(Document o) {
		if (this.score != o.score) {
			return -this.score.compareTo(o.score);
		} else {
			return this.name.compareTo(o.name);
		}
	}

	@Override
	public String toString() {
		return name + "\t" + age + "\t" + gender + "\t" + score;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public Integer getScore() {
		return score;
	}

	public void setScore(Integer score) {
		this.score = score;
	}
}
