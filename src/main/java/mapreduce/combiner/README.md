### Define a Combiner

#### The main effect of no Combiner as follows:
> First, network bandwidth is seriously occupied, reducing program efficiency.<br/><br/>
> Second, single node overloaded will reduce program performance.</br>

#### Note: When can we use Combiner?
> ①与mapper和reducer不同的是，combiner没有默认的实现，需要显式的设置在conf中才有作用。</br>

> ②并不是所有的job都适用combiner，只有操作满足结合律的才可设置combiner。combine操作类似于：opt(opt(1, 2, 3), opt(4, 5, 6))。如果opt为求和、求最大值的话，可以使用，但是如果是求中值的话，不适用。</br>

> 如果在wordcount中不用combiner，那么所有的结果都是reduce完成，效率会相对低下。使用combiner之后，先完成的map会在本地聚合，提升速度。对于hadoop自带的wordcount的例子，value就是一个叠加的数字，所以map一结束就可以进行reduce的value叠加，而不必要等到所有的map结束再去进行reduce的value叠加。</br>