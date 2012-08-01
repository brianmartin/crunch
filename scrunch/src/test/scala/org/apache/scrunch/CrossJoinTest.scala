package org.apache.scrunch

import org.scalatest.junit.JUnitSuite
import _root_.org.junit.Test

class CrossJoinTest extends JUnitSuite {
	
  @Test
  def testCrossCollection() {
    val testCases = List(Array(1,2,3,4,5), Array(6,7,8), Array.empty[Int])
    val testCasePairs = testCases flatMap {test1 => testCases map {test2 => (test1,test2)}}
    
    for ((test1, test2) <- testCasePairs) {
      val X = Mem.collectionOf(test1: _*)
      val Y = Mem.collectionOf(test2: _*)
      val cross = X.cross(Y)
      
      val crossSet = cross.materialize().toSet
      
	  assert(crossSet.size == test1.size * test2.size)
      assert(test1.flatMap(t1 => test2.map(t2 => crossSet.contains((t1, t2)))).forall(_ == true))

    }
  }
  
  @Test
  def testCrossTable() {
    val testCases = List(Array((1,2),(3,4),(5,6)), Array((7,8),(9,10)), Array.empty[(Int,Int)])
    val testCasePairs = testCases flatMap {test1 => testCases map {test2 => (test1,test2)}}
    
    for ((test1, test2) <- testCasePairs) {
      val X = Mem.tableOf(test1)
      val Y = Mem.tableOf(test2)
      val cross = X.cross(Y)
      
      val crossSet = cross.materializeToMap().toSet
	  val actualCross = test1.flatMap(t1 => test2.map(t2 => ((t1._1, t2._1), (t1._2, t2._2))))
      
	  assert(crossSet.size == test1.size * test2.size)
	  assert(actualCross.map(crossSet.contains(_)).forall(_ == true))
    }
  }

}
