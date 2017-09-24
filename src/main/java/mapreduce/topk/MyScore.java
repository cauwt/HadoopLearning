package mapreduce.topk;

public class MyScore implements Comparable<MyScore> {

	private Integer score;

	public MyScore(Integer score) {
		this.score = score;
	}

	public Integer getScore() {
		return score;
	}

	public void setScore(Integer score) {
		this.score = score;
	}

	@Override
	public int compareTo(MyScore o) {
		return score.compareTo(o.getScore());
	}

}
