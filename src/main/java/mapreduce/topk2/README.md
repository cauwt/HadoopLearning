### TopK problem

#### First group, then sort
> Please look at groupsort package.<br/>
> First, define a class 'Person' implements WritableComparable and override methods 'write', 'readFields' and 'compareTo', of course, you also should override 'toString' method so as to data transition.<br/>
	<code>public class Person implements WritableComparable<Person><code><br/>
> Second, realize a your mapper.<br/>
> Third, realize a your partitioner so as to meet the requirement group by age.<br/>
> Forth, realize a your reducer.<br/>

##### results as follows:
![]()

#### Top k
> Please look at top package.<br/>
> First, define a class 'Document' implements WritableComparable and override methods. For method 'compareTo', if score does not equal then sort by score DESC, otherwise sort by name ASC. Do not add other special logic.<br/>
> Second, define your mapper, partitioner and reducer.<br/>
> Note: Reducer code as follows:
	
	public static class MyReducer extends Reducer<Document, NullWritable, Document, NullWritable> {

		private int k = 3;
		private int counter = 0;

		@Override
		protected void reduce(Document key, Iterable<NullWritable> v2s, Context context)
				throws IOException, InterruptedException {

			log.info("MyReducer in<" + key + ">");

			if (counter < k) {
				context.write(key, NullWritable.get());
				counter += 1;

				log.info("MyReducer out<" + key + ">");
			}
		}
	}
	
##### results as follows:
![]()