package org.example
package models

import java.time.{LocalDate, LocalDateTime}

case class Trip(
                 tripId: Integer,
                 duration: Integer,
                 startDate: LocalDate,
                 startStation: String,
                 startTerminal: Integer,
                 endDate: LocalDate,
                 endStation: String,
                 endTerminal: Integer,
                 bikeId: Integer,
                 subscriptionType: String,
                 zipCode: String
               )
