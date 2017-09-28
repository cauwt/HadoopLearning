### Define a Partitioner

> We can define a partitioner if biz need. Only extends Partitioner and override the method 'getPartition'. Then, we need do 'setpartitionerClass' and setNumReduceTasks(2) in our run method, because we need to depart telephone or non-telephone into different file.

#### The main usage of Partitioner as follows:
> First, generate multiple output files as required by the business.<br/>
> Second, multiple reduce tasks will run concurrently to improve overall job operating efficiency.<br/>
