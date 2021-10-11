package org.example
package models

case class Station(
                    stationId:Integer,
                    name: String,
                    lat: Double,
                    long: Double,
                    dockcount: Integer,
                    landmark: String,
                    installation: String,
                    notes: String
                  )
