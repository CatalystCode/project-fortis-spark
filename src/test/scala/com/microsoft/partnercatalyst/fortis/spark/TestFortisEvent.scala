package com.microsoft.partnercatalyst.fortis.spark

import com.microsoft.partnercatalyst.fortis.spark.dto.{Analysis, Details, FortisEvent, Location}

case class TestFortisEvent(
  details: Details,
  analysis: Analysis
) extends FortisEvent

case class TestFortisDetails(
<<<<<<< HEAD
                              eventid: String,
                              eventtime: Long,
                              body: String,
                              title: String,
                              pipelinekey: String,
                              sourceurl: String,
                              sharedLocations: List[Location] = List()
=======
                              id: String,
                              eventtime: Long,
                              body: String,
                              externalsourceid: String,
                              title: String,
                              pipelinekey: String,
                              sourceurl: String,
                              sharedlocations: List[Location] = List()
>>>>>>> fcc3ccc507f888983c834b62436ab3481ae635ab
) extends Details