package com.banzaicloud.spark.metrics.filters

import java.util.regex.Pattern
import com.codahale.metrics.{Metric, MetricFilter}

class RegexMetricFilter(val props: Map[String, String]) extends MetricFilter {
  private val patternString: String = props.getOrElse("regex", ".*")
  private val pattern: Pattern = Pattern.compile(patternString)

  override def matches(name: String, metric: Metric): Boolean = {
    name != null && pattern.matcher(name).matches()
  }
}
