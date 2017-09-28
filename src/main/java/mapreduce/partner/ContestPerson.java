package mapreduce.partner;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yachao on 17/9/27.
 */
public class ContestPerson implements WritableComparable<ContestPerson> {

	private String name;

	private Integer age;

	private String gender;

	private Integer score;

	public ContestPerson() {
	}

	public ContestPerson(String name, Integer age, String gender, Integer score) {
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
	public String toString() {
		return "\t" + age + "\t" + gender + "\t" + score;
	}

	@Override
	public int compareTo(ContestPerson o) {
		if (!this.gender.equals(o.getGender())) {
			return this.gender.compareTo(o.getGender());
		} else if (this.age != o.getAge()) {
			return this.age.compareTo(o.getAge());
		} else if (this.score != o.getScore()) {
			return -this.score.compareTo(o.getScore());
		} else {
			return this.name.compareTo(o.getName());
		}
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
