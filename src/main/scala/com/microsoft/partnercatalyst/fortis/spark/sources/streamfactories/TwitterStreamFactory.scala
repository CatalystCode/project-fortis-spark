package com.microsoft.partnercatalyst.fortis.spark.sources.streamfactories

import com.microsoft.partnercatalyst.fortis.spark.dba.ConfigurationManager
import com.microsoft.partnercatalyst.fortis.spark.logging.Loggable
import com.microsoft.partnercatalyst.fortis.spark.sources.streamfactories.TwitterStreamFactory._
import com.microsoft.partnercatalyst.fortis.spark.sources.streamprovider.ConnectorConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{FilterQuery, Status}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TwitterStreamFactory(configurationManager: ConfigurationManager) extends StreamFactoryBase[Status] with Loggable {

  private[streamfactories] var TwitterMaxTermCount = sys.env.getOrElse("FORTIS_TWITTER_MAX_TERM_COUNT", 400.toString).toInt
  private[streamfactories] var TwitterMaxTermBytes = 60

  override protected def canHandle(connectorConfig: ConnectorConfig): Boolean = {
    connectorConfig.name == "Twitter"
  }

  override protected def buildStream(ssc: StreamingContext, connectorConfig: ConnectorConfig): DStream[Status] = {
    import ParameterExtensions._

    val params = connectorConfig.parameters
    val consumerKey = params.getAs[String]("consumerKey")
    val auth = new OAuthAuthorization(
      new ConfigurationBuilder()
        .setOAuthConsumerKey(consumerKey)
        .setOAuthConsumerSecret(params.getAs[String]("consumerSecret"))
        .setOAuthAccessToken(params.getAs[String]("accessToken"))
        .setOAuthAccessTokenSecret(params.getAs[String]("accessTokenSecret"))
        .build()
    )

    val query = new FilterQuery

    if (params.getOrElse("watchlistFilteringEnabled", "true").toString.toBoolean) {
      val keywordsAdded = appendWatchlist(query, ssc.sparkContext, configurationManager)
      if (!keywordsAdded) {
        logInfo(s"No keywords used for Twitter consumerKey $consumerKey. Returning empty stream.")
        return ssc.queueStream(new mutable.Queue[RDD[Status]])
      }
    }

    val languagesAdded = addLanguages(query, ssc.sparkContext, configurationManager)
    if (!languagesAdded) {
      logInfo(s"No languages set for Twitter consumerKey $consumerKey. Returning empty stream.")
      return ssc.queueStream(new mutable.Queue[RDD[Status]])
    }

    val stream = TwitterUtils.createFilteredStream(
      ssc,
      twitterAuth = Some(auth),
      query = Some(query)
    )

    val trustedSourceScreenNames = params.getTrustedSources.toSet
    stream.filter(status=>{
      def isOriginalTweet(status: Status) : Boolean = {
        !status.isRetweet && status.getRetweetedStatus == null
      }

      if (!isOriginalTweet(status)) {
        false
      } else {
        if (trustedSourceScreenNames.isEmpty) {
          true
        } else {
          trustedSourceScreenNames.contains(status.getUser.getScreenName)
        }
      }
    })
  }

  private[streamfactories] def appendWatchlist(query: FilterQuery, sparkContext: SparkContext, configurationManager: ConfigurationManager): Boolean = {
    val watchlistCurrentOffsetKey = "TwitterStreamFactory.watchlistCurrentOffset"
    val watchlistCurrentOffsetValue = sparkContext.getLocalProperty(watchlistCurrentOffsetKey) match {
      case value:String => value.toInt
      case _ => 0
    }

    val watchlist = configurationManager.fetchWatchlist(sparkContext)
    val sortedTerms = watchlist.values.flatten.toList.sorted

    val terms = sortedTerms.drop(watchlistCurrentOffsetValue)
    if (terms.isEmpty) return false

    val phraseGroupsWithIndex = getPhraseGroups(terms).toIterator.zipWithIndex.buffered
    val phrases = new ListBuffer[String]

    def nextCanFit: Boolean = {
      phrases.length + phraseGroupsWithIndex.head._1.length <= TwitterMaxTermCount
    }

    while (phraseGroupsWithIndex.hasNext && nextCanFit) {
      phrases.appendAll(phraseGroupsWithIndex.next()._1)
    }

    query.track(phrases:_*)

    val updatedWatchlistCurrentOffsetValue =
      watchlistCurrentOffsetValue + (if (phraseGroupsWithIndex.hasNext) phraseGroupsWithIndex.head._2 else terms.length)

    sparkContext.setLocalProperty(watchlistCurrentOffsetKey, updatedWatchlistCurrentOffsetValue.toString)

    true
  }

  private[streamfactories] def getPhraseGroups(terms: List[String]): List[List[String]] = {
    def groupFromSplit(segments: List[String]): List[String] = {
      /**
        * Evenly fills 'bins' with 'remainingTerms'.
        * @param bins A list of mutable buffers (bins) to fill.
        * @param remainingTerms Terms that must be added to bins.
        * @return True if all terms were placed, false otherwise. If false, the state of 'bins' is undefined.
        */
      @tailrec def putTerms(bins: List[ListBuffer[String]], remainingTerms: List[String]): Boolean = {
        remainingTerms.headOption match {
          case Some(head) =>
            val targetBinOpt = bins.sorted(BinOrdering).collectFirst {
              case bin if getBinSize(head :: bin.toList) <= TwitterMaxTermBytes => bin
            }

            targetBinOpt match {
              case Some(targetBin) =>
                targetBin.append(head)
                putTerms(bins, remainingTerms.tail)
              case None =>
                false
            }
          case None =>
            true
        }
      }

      /**
        * Attempt to distribute segments into the minimum number of bins >= numBins.
        * @param numBins minimum number of bins to distribute into.
        * @return filled bins.
        */
      @tailrec def distributeOverBins(sortedSegments: List[String], numBins: Int = 1): List[ListBuffer[String]] = {
        val bins = List.fill(numBins)(new ListBuffer[String]())
        if (putTerms(bins, sortedSegments))
          bins
        else
          distributeOverBins(sortedSegments, numBins + 1)
      }

      val sortedSegments = segments.sortBy(_.getBytes.length)(Ordering[Int].reverse)
      distributeOverBins(sortedSegments).map(_.mkString(" "))
    }

    terms.map(term =>
      term.getBytes.length match {
        case length if length <= TwitterMaxTermBytes =>
          List(term)

        case _ =>
          // Term is too big for API. Split into buckets.
          val split = term.split("\\s+")

          if (split.exists(_.getBytes.length > TwitterMaxTermBytes)) {
            logError(s"Ignoring invalid term which contains a single word > $TwitterMaxTermBytes bytes.")
            List()
          } else {
            groupFromSplit(split.toList) match {
              case group if group.length <= TwitterMaxTermCount => group
              case _ =>
                logError("Ignoring invalid term which contains more phrases than Twitter API can handle in a single request.")
                List()
            }
          }
      }
    )
  }

  private[streamfactories] def addLanguages(query: FilterQuery, sparkContext: SparkContext, configurationManager: ConfigurationManager): Boolean = {
    val defaultlanguage = configurationManager.fetchSiteSettings(sparkContext).defaultlanguage
    val languages = configurationManager.fetchSiteSettings(sparkContext).languages
    val allLanguages = defaultlanguage match {
      case None => languages
      case Some(language) => (Set(language) ++ languages.toSet).toSeq
    }

    allLanguages.size match {
      case 0 => false
      case _ =>
        query.language(allLanguages:_*)
        true
    }
  }
}

object TwitterStreamFactory {
  def parseLocations(params: Map[String, Any]): Option[Array[Array[Double]]] = {
    parseList(params, "locations").map(_.map(_.split(','))) match {
      case None => None
      case Some(locations) if locations.exists(_.length != 4) => None
      case Some(locations) => Some(locations.map(_.map(_.toDouble)))
    }
  }

  private def parseList(params: Map[String, Any], key: String): Option[Array[String]] = params.get(key).map(_.asInstanceOf[String].split('|'))

  private def getBinSize(bin: Seq[String]): Int = {
    bin.map(_.getBytes.length).reduceOption(_ + _) match {
      case Some(sizeBytes) => sizeBytes + (bin.length - 1) * " ".getBytes.length
      case None => 0
    }
  }

  private object BinOrdering extends Ordering[ListBuffer[String]] {
    /**
      * A primary ordering in which bins with fewer items come first, and a secondary ordering
      * (when same number of items) in which bins with greatest byte size come first.
      */
    override def compare(x: ListBuffer[String], y: ListBuffer[String]): Int = {
      Ordering[Int].compare(x.length, y.length) match {
        case diffLength if diffLength != 0 =>
          diffLength
        case _ =>
          Ordering[Int].reverse.compare(getBinSize(x), getBinSize(y))
      }
    }
  }
}
