import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkGraphSocialNetwork extends App {
  // Initialize spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf: SparkConf = new SparkConf().setAppName("Histogram").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)

  // Set up graph
  val vertexArray = Array(
    (1L, ("Alice", 28)),
    (2L, ("Bob", 27)),
    (3L, ("Charlie", 65)),
    (4L, ("David", 42)),
    (5L, ("Ed", 55)),
    (6L, ("Fran", 50))
  )

  val edgeArray = Array(
    Edge(2L, 1L, 7),
    Edge(2L, 4L, 2),
    Edge(3L, 2L, 4),
    Edge(3L, 6L, 3),
    Edge(4L, 1L, 1),
    Edge(5L, 2L, 2),
    Edge(5L, 3L, 8),
    Edge(5L, 6L, 3)
  )

  val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
  val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
  val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)


  // Task 1
  println("\nTask 1\n")
  graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))

  // Task 2
  println("\nTask 2\n")
  for (triplet <- graph.triplets) {
    println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
  }

  // Task 3
  println("\nTask 3\n")
  for (triplet <- graph.triplets.filter((triplet) => triplet.attr >= 5).collect()) {
    println(s"${triplet.srcAttr._1} loves ${triplet.dstAttr._1}")
  }

  // Task 4
  println("\nTask 4\n")
  for (triplet <- graph.triplets.filter((triplet) => triplet.attr >= 5).collect()) {
    println(s"${triplet.srcAttr._1} loves ${triplet.dstAttr._1}")
  }

  // Task 5
  println("\nTask 5\n")

  // Define a class to more clearly model the user property
  case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

  // Create a user Graph
  val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0) }

  // Fill in the degree information
  val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
    case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), 0)
  }.outerJoinVertices(graph.outDegrees) {
    case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
  }

  // Task 6
  println("\nTask 6\n")

  for ((id, property: User) <- userGraph.vertices.collect) {
    println(s"User $id is called ${property.name} and is liked by ${property.inDeg} people.")
  }

  // Task 7
  println("\nTask 7\n")

  userGraph.vertices.filter {
    case (id, u) => (u.inDeg == u.outDeg)
  }.collect.foreach {
    case (id, property) => println(property.name)
  }

  // Task 8
  println("\nTask 8")
  val oldestFollower: VertexRDD[(String, Int)] = userGraph.aggregateMessages[(String, Int)](
    triplet => (triplet.sendToDst(triplet.srcAttr.name, triplet.srcAttr.age)),
    (a, b) => (if (a._2 > b._2) a else b)
  )

  userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
    optOldestFollower match {
      case None => s"${user.name} does not have any followers."
      case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
    }
  }.collect.foreach { case (id, str) => println(str) }

  //          THEM                                                      ME
  //  David is the oldest follower of Alice.          David is the oldest follower of Alice.
  //  Charlie is the oldest follower of Bob.          Charlie is the oldest follower of Bob.
  //  Ed is the oldest follower of Charlie.           Ed is the oldest follower of Charlie.
  //  Bob is the oldest follower of David.            Bob is the oldest follower of David.
  //  Ed does not have any followers.                 Ed does not have any followers.
  //  Charlie is the oldest follower of Fran.         Charlie is the oldest follower of Fran.

  // Task 9
  println("\nTask 9")
  val averageAge: VertexRDD[Double] = userGraph.aggregateMessages[(Int, Double)](
    // map function returns a tuple of (1, Age)
    triplet => (triplet.sendToDst(1, triplet.srcAttr.age)),
    // reduce function combines (sumOfFollowers, sumOfAge)
    (a, b) => (a._1 + b._1, a._2 + b._2)
  ).mapValues((id, p) => p._2 / p._1)

  // Display the results
  userGraph.vertices.leftJoin(averageAge) { (id, user, optAverageAge) =>
    optAverageAge match {
      case None => s"${user.name} does not have any followers."
      case Some(avgAge) => s"The average age of ${user.name}\'s followers is $avgAge."
    }
  }.collect.foreach { case (id, str) => println(str) }

  // Task 10
  println("\nTask 10")
  val olderGraph = userGraph.subgraph((triplet) => (triplet.srcAttr.age > 30))

  // compute the connected components
  val cc = olderGraph.connectedComponents

  // display the component id of each user:
  olderGraph.vertices.leftJoin(cc.vertices) {
    case (id, user, comp) => s"${user.name} is in component ${comp.get}"
  }.collect.foreach { case (id, str) => println(str) }
}