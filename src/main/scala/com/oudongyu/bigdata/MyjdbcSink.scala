package com.oudongyu.bigdata

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MyjdbcSink extends RichSinkFunction[SensorReading] {
  var conn: Connection = _
  var insertStatement: PreparedStatement = _
  var updateStatement: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    insertStatement = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?, ?)")
    updateStatement = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
  }

  override def close(): Unit = super.close()

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStatement.setDouble(1, value.temperature)
    updateStatement.setString(2, value.id)
    updateStatement.execute()

    if (updateStatement.getUpdateCount == 0) {
      insertStatement.setString(1, value.id)
      insertStatement.setDouble(2, value.temperature)
      insertStatement.execute()
    }
  }
}
