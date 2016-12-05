package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */

  def checkMethod(ch: Char): Int = {
    if (ch == '(')
      1
    else if (ch == ')')
      -1
    else 0
  }


  def balance(chars: Array[Char]): Boolean = {
    @tailrec
    def auxiliaryBalance(achars: List[Char], counter: Int): Int = {
      achars match {
        case ch :: tail if counter >= 0 => auxiliaryBalance(tail, counter + checkMethod(ch))
        case _ => counter
      }
    }
    if (chars.isEmpty) true
    else {
      auxiliaryBalance(chars.toList, 0) == 0
    }
  }


  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {
    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): Int = {
      var counter = arg1
      var idxTemp = idx
      while (idxTemp < until) {
        if (counter >= 0) {
          counter += checkMethod(chars(idxTemp))
        }
        idxTemp += 1
      }
      counter
    }

    def reduce(from: Int, until: Int): Int = {
      val size = until - from
      if (size <= threshold) {
        traverse(from, until, 0, 0)
      } else {
        val mid = size / 2
        val (a, b) = parallel(reduce(from, from + mid), reduce(from + mid, until))
        a + b
      }
    }
    if (checkMethod(chars(0)) == -1) {
      false
    } else
      reduce(0, chars.length) == 0
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
