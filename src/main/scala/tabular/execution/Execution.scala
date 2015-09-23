package tabular.execution

import java.util.concurrent.atomic.AtomicLong

/**
 * Created by tiong on 8/20/15.
 */
abstract class ExecutionStep[OUT] {
  def execute(): OUT
}

class IdentityStep[T](t: => T) extends ExecutionStep[T] {
  override def execute(): T = t
}

class ChainedStep[IN, OUT](val desc: String, prev: ExecutionStep[IN], func: (IN) => OUT) extends ExecutionStep[OUT] {
  override def execute(): OUT = {
    "Execuing " + desc
    val in = prev.execute()
    func(in)
  }
}

/**
 * Represent an execution plan
 * @param desc
 */
class ExecutionPlan[That](desc: String, lastStep: ExecutionStep[That]) {

  import tabular.execution.ExecutionPlanning._

  /** a unique id **/
  val id = ID_COUNTER.addAndGet(1)

  def execute(): That = lastStep.execute()
}

object ExecutionPlanning {
  /** id counter **/
  val ID_COUNTER = new AtomicLong()
  val planContext = new ThreadLocal[ExecutionStep[_]]


  def Plan[T](name: String)(planFunc: => Unit): ExecutionPlan[T] =  {
    if (planContext.get!=null){
      throw new IllegalStateException()
    }
    try {
      planFunc
      new ExecutionPlan(name, planContext.get.asInstanceOf[ExecutionStep[T]])
    } finally {
      planContext.set(null)
    }
  }

  def Step[That](name: String)(func: => That): ExecutionStep[That] = {
    val step = new IdentityStep(func)
    planContext.set(step)
    step
  }

  def Step[This, That](name: String)(func: (This) => That) {
    val lastStep = planContext.get()
    lastStep match {
      case null =>
        throw new IllegalStateException("No previous step found")
      case someStep: ExecutionStep[This] =>
        val newStep = new ChainedStep(name, someStep, func)
        planContext.set(newStep)
      case oddStep =>
        new IllegalStateException("Incompatible step " + oddStep)
    }
  }

  def operate[T](data: T) = {
    println(data)
  }

  def main(args: Array[String]) {
    val p = Plan("test") {
      Step[Int]("One"){
        1
      }
      Step[Int, Int]("Two"){
        _ + 1
      }
    }
    println(p)
    operate(1)
  }
}