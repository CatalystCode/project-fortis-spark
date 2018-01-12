package com.microsoft.partnercatalyst.fortis.spark.sources.streamfactories

import com.microsoft.partnercatalyst.fortis.spark.dba.ConfigurationManager
import com.microsoft.partnercatalyst.fortis.spark.dto.{Geofence, SiteSettings}
import org.apache.spark.{SparkConf, SparkContext}
import org.mockito.Mockito
import org.mockito.ArgumentMatchers
import org.scalatest.{BeforeAndAfter, FlatSpec}
import twitter4j.FilterQuery

/**
  * TODO: A lot of these test are very brittle in that FilterQuery track parameter order matters in comparisons.
  * Ideally, this should be fixed in Twitter4j library (track List should be a Set).
  */
class TwitterStreamFactorySpec extends FlatSpec with BeforeAndAfter {

  private val conf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local[*]")
    .set("output.consistency.level", "LOCAL_ONE")

  private var sc: SparkContext = _
  private var factory: TwitterStreamFactory = _
  private var configurationManager: ConfigurationManager = _
  private var siteSettings: SiteSettings = _

  before {
    sc = new SparkContext(conf)
    factory = new TwitterStreamFactory(configurationManager)
    configurationManager = Mockito.mock(classOf[ConfigurationManager])
    siteSettings = new SiteSettings(
      sitename = "Fortis",
      geofence = Seq(1, 2, 3, 4),
      defaultlanguage = Some("en"),
      languages = Seq("en", "es", "fr"),
      defaultzoom = 8,
      featureservicenamespace = Some("somenamespace"),
      title = "Fortis",
      logo = "",
      translationsvctoken = "",
      cogspeechsvctoken = "",
      cogvisionsvctoken = "",
      cogtextsvctoken = "",
      insertiontime = System.currentTimeMillis()
    )

    Mockito.when(configurationManager.fetchSiteSettings(ArgumentMatchers.any())).thenReturn(siteSettings)
  }

  after {
    sc.stop()
  }

  it should "append to query when watchlist is present" in {
    Mockito.when(configurationManager.fetchWatchlist(ArgumentMatchers.any())).thenReturn(Map(
      "en"->Seq("hello", "world"),
      "es"->Seq("hola", "mundo"),
      "fr"->Seq("salut", "monde")
    ))

    val query = new FilterQuery()
    val watchlistAppended = factory.appendWatchlist(query, sc, configurationManager)
    assert(watchlistAppended)
    assert(query == new FilterQuery("hello", "hola", "monde", "mundo", "salut", "world"))
  }

  it should "append subset to query when watchlist count exceeds max term count" in {
    Mockito.when(configurationManager.fetchWatchlist(ArgumentMatchers.any())).thenReturn(Map(
      "en"->Seq("hello", "world"),
      "es"->Seq("hola", "mundo"),
      "fr"->Seq("salut", "monde")
    ))

    factory.TwitterMaxTermCount = 2

    val query0 = new FilterQuery()
    assert(factory.appendWatchlist(query0, sc, configurationManager))
    assert(query0 == new FilterQuery("hello", "hola"))

    val query1 = new FilterQuery()
    assert(factory.appendWatchlist(query1, sc, configurationManager))
    assert(query1 == new FilterQuery("monde", "mundo"))

    val query2 = new FilterQuery()
    assert(factory.appendWatchlist(query2, sc, configurationManager))
    assert(query2 == new FilterQuery("salut", "world"))
  }

  it should "properly handle terms greater than TwitterMaxTermBytes bytes" in {
    Mockito.when(configurationManager.fetchWatchlist(ArgumentMatchers.any())).thenReturn(Map(
      "es"->Seq("hola", "programa nacional integral de sustitución de cultivos ilícitos"),
      "fr"->Seq("salut")
    ))

    factory.TwitterMaxTermCount = 4
    factory.TwitterMaxTermBytes = 20

    val query0 = new FilterQuery()
    assert(factory.appendWatchlist(query0, sc, configurationManager))
    assert(query0 == new FilterQuery("hola"))

    val query1 = new FilterQuery()
    assert(factory.appendWatchlist(query1, sc, configurationManager))
    assert(query1 == new FilterQuery("sustitución de", "ilícitos integral", "programa cultivos", "nacional de"))

    val query2 = new FilterQuery()
    assert(factory.appendWatchlist(query2, sc, configurationManager))
    assert(query2 == new FilterQuery("salut"))
  }

  it should "allow terms with words of size == TwitterMaxTermBytes bytes" in {
    Mockito.when(configurationManager.fetchWatchlist(ArgumentMatchers.any())).thenReturn(Map(
      "es"->Seq("sustitución")
    ))

    factory.TwitterMaxTermBytes = 12

    val query0 = new FilterQuery()
    assert(factory.appendWatchlist(query0, sc, configurationManager))
    assert(query0 == new FilterQuery("sustitución"))
  }

  it should "skip terms with any words of size > TwitterMaxTermBytes bytes" in {
    Mockito.when(configurationManager.fetchWatchlist(ArgumentMatchers.any())).thenReturn(Map(
      "es"->Seq("programa de sustitución", "integral ilícitos")
    ))

    factory.TwitterMaxTermBytes = 11

    val query0 = new FilterQuery()
    assert(factory.appendWatchlist(query0, sc, configurationManager))
    assert(query0 == new FilterQuery("ilícitos", "integral"))
  }

  it should "skip terms that split into > TwitterMaxTermCount phrases" in {
    Mockito.when(configurationManager.fetchWatchlist(ArgumentMatchers.any())).thenReturn(Map(
      "es"->Seq("hola", "programa nacional integral de sustitución de cultivos ilícitos"),
      "fr"->Seq("salut")
    ))

    factory.TwitterMaxTermCount = 3
    factory.TwitterMaxTermBytes = 20

    val query0 = new FilterQuery()
    assert(factory.appendWatchlist(query0, sc, configurationManager))
    assert(query0 == new FilterQuery("hola", "salut"))
  }

  it should "return false when watchlist is absent" in {
    Mockito.when(configurationManager.fetchWatchlist(ArgumentMatchers.any())).thenReturn(Map[String, Seq[String]]())
    val query = new FilterQuery()
    val watchlistAppended = factory.appendWatchlist(query, sc, configurationManager)
    assert(!watchlistAppended)
    assert(query == new FilterQuery())
  }

  it should "append to query when languages are present" in {
    val query = new FilterQuery()
    val languagesAdded = factory.addLanguages(query, sc, configurationManager)
    assert(languagesAdded)
    val expectedQuery = new FilterQuery()
    expectedQuery.language("en", "es", "fr")
    assert(query == expectedQuery)
  }

  it should "return true when languages are absent but defaultlanguage is present" in {
    val noLanguageSiteSettings = SiteSettings(
      sitename = "Fortis",
      geofence = Seq(1, 2, 3, 4),
      defaultlanguage = Some("en"),
      languages = Seq(),
      defaultzoom = 8,
      featureservicenamespace = Some("somenamespace"),
      title = "Fortis",
      logo = "",
      translationsvctoken = "",
      cogspeechsvctoken = "",
      cogvisionsvctoken = "",
      cogtextsvctoken = "",
      insertiontime = System.currentTimeMillis()
    )

    Mockito.when(configurationManager.fetchSiteSettings(ArgumentMatchers.any())).thenReturn(noLanguageSiteSettings)

    val query = new FilterQuery()
    val languagesAdded = factory.addLanguages(query, sc, configurationManager)
    assert(languagesAdded)
    val expectedQuery = new FilterQuery()
    expectedQuery.language("en")
    assert(query == expectedQuery)
  }

  it should "return false when both languages and defaultlanguage are absent" in {
    val noLanguageSiteSettings = new SiteSettings(
      sitename = "Fortis",
      geofence = Seq(1, 2, 3, 4),
      defaultlanguage = None,
      languages = Seq(),
      defaultzoom = 8,
      featureservicenamespace = Some("somenamespace"),
      title = "Fortis",
      logo = "",
      translationsvctoken = "",
      cogspeechsvctoken = "",
      cogvisionsvctoken = "",
      cogtextsvctoken = "",
      insertiontime = System.currentTimeMillis()
    )

    Mockito.when(configurationManager.fetchSiteSettings(ArgumentMatchers.any())).thenReturn(noLanguageSiteSettings)

    val query = new FilterQuery()
    val languagesAdded = factory.addLanguages(query, sc, configurationManager)
    assert(!languagesAdded)
    assert(query == new FilterQuery())
  }

  it should "parse locations" in {
    assert(TwitterStreamFactory.parseLocations(Map("foo" -> "bar")).isEmpty)
    assert(TwitterStreamFactory.parseLocations(Map("locations" -> "-74,40,-73")).isEmpty)
    assert(TwitterStreamFactory.parseLocations(Map("locations" -> "-74,40|-73,41")).isEmpty)
    assert(TwitterStreamFactory.parseLocations(Map("locations" -> "-122.75,36.8,-121.75,37.8|40,-73,41")).isEmpty)
    assert(TwitterStreamFactory.parseLocations(Map("locations" -> "-74,40,-73,41")).get.length == 1)
    assert(TwitterStreamFactory.parseLocations(Map("locations" -> "-74,40,-73,41")).get(0) sameElements Array(-74D, 40D, -73D, 41D))
    assert(TwitterStreamFactory.parseLocations(Map("locations" -> "-122.75,36.8,-121.75,37.8|-74,40,-73,41")).get.length == 2)
    assert(TwitterStreamFactory.parseLocations(Map("locations" -> "-122.75,36.8,-121.75,37.8|-74,40,-73,41")).get(0) sameElements Array(-122.75D, 36.8D, -121.75D, 37.8D))
    assert(TwitterStreamFactory.parseLocations(Map("locations" -> "-122.75,36.8,-121.75,37.8|-74,40,-73,41")).get(1) sameElements Array(-74D, 40D, -73D, 41D))
  }
}
