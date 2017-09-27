package mapreduce.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * 
 * <p>Title: Information</p>
 * <p>Description: </p>
 * @author jangz
 * @date 2017年9月27日 下午3:04:01
 */
public class Information implements WritableComparable<Information> {

	private String account;

	private double income;

	private double expenses;

	private double surplus;

	public void set(String account, double income, double expenses) {
		this.account = account;
		this.income = income;
		this.expenses = expenses;
		this.surplus = income - expenses;
	}

	@Override
	public String toString() {
		return this.income + "\t" + this.expenses + "\t" + this.surplus;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(account);
		out.writeDouble(income);
		out.writeDouble(expenses);
		out.writeDouble(surplus);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.account = in.readUTF();
		this.income = in.readDouble();
		this.expenses = in.readDouble();
		this.surplus = in.readDouble();
	}

	@Override
	public int compareTo(Information info) {
		if (this.income == info.getIncome()) {
			return this.expenses > info.getExpenses() ? 1 : -1;
		} else {
			return this.income > info.getIncome() ? -1 : 1;
		}
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public double getIncome() {
		return income;
	}

	public void setIncome(double income) {
		this.income = income;
	}

	public double getExpenses() {
		return expenses;
	}

	public void setExpenses(double expenses) {
		this.expenses = expenses;
	}

	public double getSurplus() {
		return surplus;
	}

	public void setSurplus(double surplus) {
		this.surplus = surplus;
	}

}
