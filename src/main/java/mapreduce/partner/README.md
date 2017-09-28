### ContestSort

> When we use a bean as key and the bean extends WritableComparable, as a result, we only need to override compareTo to realize sort, the method 'setSortComparatorClass' need to remove. 

#### The implementation of method 'compareTo' as follows:
	    @Override
    public int compareTo(ContestPerson o) {
        if (!this.gender.equals(o.getGender())) {
            return this.gender.compareTo(o.getGender());
        } else if (this.score != o.getScore()) {
            return this.score.compareTo(o.getScore());
        } else if (this.age != o.getAge()) {
            return this.age.compareTo(o.getAge());
        } else {
            return this.name.compareTo(o.getName());
        }
    }

#### members.txt:
Alice,23,female,45<br/>
Bob,34,male,89<br/>
Chris,67,male,97<br/>
Kristine,38,female,53<br/>
Connor,25,male,27<br/>
Daniel,78,male,95<br/>
James,34,male,79<br/>
Alex,52,male,69<br/>
Nance,7,female,98<br/>
Adam,9,male,37<br/>
Jacob,7,male,23<br/>
Mary,6,female,93<br/>
Clara,87,female,72<br/>
Monica,56,female,92<br/>

#### result as follows:
![](https://github.com/Zychaowill/ImgStore/blob/master/hadoop/屏幕快照%202017-09-27%20下午11.47.19.png)