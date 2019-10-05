import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object SparkGraphSong extends App {
  // Initialize spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf: SparkConf = new SparkConf().setAppName("Histogram").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)

  // Task 1
  println("\nTask 1\n")

  case class Song(title: String, artist: String, tags: Set[String])

  val fileName = "data/song_hash.txt"
  var songs: RDD[(VertexId, Song)] = sc.textFile(fileName).map((line) => {
    var x = line.split("\t");
    (x(0).toLong, Song(x(1), x(2), Set()))
  });

  songs.take(5).foreach(println)

  // Task 2
  println("\nTask 2\n")

  val graphFromSongs: Graph[Song, Int] = Graph.apply(songs, sc.parallelize(Array.empty[Edge[Int]]))
  graphFromSongs.vertices.take(5).foreach(println)

  // Task 3
  println("\nTask 3\n")

  val tagFile = "data/tags.txt"
  val source = Source.fromFile(tagFile);
  val tagIter: Iterator[(VertexId, Set[String])] = source.getLines().zipWithIndex.map { case (x) => (x._2, x._1.split(" ").toSet) }
  val tagRDD = sc.parallelize(tagIter.toSeq)

  tagRDD.take(5).foreach(println)

  // Task 4
  println("\nTask 4\n")

  val taghashFile = "data/tag_hash.txt"
  val sourceTag = Source.fromFile(taghashFile);
  val tags: Map[Int, String] = sourceTag.getLines().foldLeft(
    Map[Int, String]()
  ) { (m, s) =>
    var split = s.split(", ")
    var id = split(0).toInt
    var description = split(1)
    m + (id -> description)
  }

  val songsNtags = graphFromSongs.joinVertices(tagRDD) {
    (id, s, ks) =>
      ks.toList match {
        case List("#") => s
        case _ =>
          val songTags = ks.map(_.toInt).flatMap(tags.get)
          Song(s.title, s.artist, songTags.toSet)
      }
  }

  songsNtags.vertices.take(3).foreach(println)

  // Task 5
  println("\nTask 5\n")


  def similarity(one: Song, other: Song): Double = {
    val numCommonTags = (one.tags intersect other.tags).size
    val numTotalTags = (one.tags union other.tags).size
    if (numTotalTags > 0)
      numCommonTags.toDouble / numTotalTags.toDouble
    else
      0.0
  }

  def quiteSimilar(one: Song, other: Song, threshold: Double): Boolean = {
    val commonTags = one.tags.intersect(other.tags)
    val combinedTags = one.tags.union(other.tags)
    commonTags.size > combinedTags.size * threshold
  }

  def differentSong(one: Song, other: Song): Boolean = one.title != other.title || one.artist != other.artist

  // First, get the songs with tags
  songs = songsNtags.vertices

  // Then, compute the similarity between each pair of songs with a similarity score larger than 0.7
  val similarConnections: RDD[Edge[Double]] = {
    val ss = songs.cartesian(songs)


    // similarSongs are the songs with a similarity score larger than 0.7
    // Note that not compare a song with itself (use the differentSong method)
    val similarSongs = ss.filter { input => {
      val (v1, s1) = input._1
      val (v2, s2) = input._2

      differentSong(s1, s2) && quiteSimilar(s1, s2, 0.7);
    }
    }

    // Now compute the Jaccard metric for the similarSongs. The result should ba an Edge for each pair of songs
    // with the vertexIds at the two ends, and the Javvard values as the edge value.
    similarSongs.map(input => {
      val (v1, s1) = input._1
      val (v2, s2) = input._2
      val jacIdx = similarity(s1, s2)
      Edge(v1, v2, jacIdx)
    })
  }

  println(similarConnections.count);

  val similarByTagsGraph = Graph(songs, similarConnections)
  val similarHighTagsGraph = similarByTagsGraph.subgraph((triplet) => {
    triplet.srcAttr.tags.size >= 5
  })

  similarHighTagsGraph.triplets.take(6).foreach(t =>
    println(t.srcAttr + " ~~~ " + t.dstAttr + " => " + t.attr)
  )
}
