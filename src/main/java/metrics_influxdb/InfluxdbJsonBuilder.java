package metrics_influxdb;

class InfluxdbJsonBuilder {
  private final StringBuilder json = new StringBuilder();

  /**
   * Forget previous appendSeries.
   */
  public void resetJson() {
    json.setLength(0);
    json.append('[');
  }

  public void endJson() {
    json.append(']');
  }

  /**
   * Append series of data into the next Request to send.
   * 
   * @param namePrefix
   * @param name
   * @param nameSuffix
   * @param columns
   * @param points
   */
  public void appendSeries(String namePrefix, String name, String nameSuffix, String[] columns, Object[][] points) {
    if (json.length() > 1)
      json.append(',');
    json.append("{\"name\":\"").append(namePrefix).append(name).append(nameSuffix).append("\",\"columns\":[");
    for (int i = 0; i < columns.length; i++) {
      if (i > 0)
        json.append(',');
      json.append('"').append(columns[i]).append('"');
    }
    json.append("],\"points\":[");
    for (int i = 0; i < points.length; i++) {
      if (i > 0)
        json.append(',');
      Object[] row = points[i];
      json.append('[');
      for (int j = 0; j < row.length; j++) {
        if (j > 0)
          json.append(',');
        Object value = row[j];
        if (value instanceof String) {
          json.append('"').append(value).append('"');
        } else {
          json.append(value);
        }
      }
      json.append(']');
    }
    json.append("]}");
  }

  @Override
  public String toString() {
    return json.toString();
  }
  
}
