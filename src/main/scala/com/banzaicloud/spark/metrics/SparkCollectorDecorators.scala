package com.banzaicloud.spark.metrics

import java.util
import com.banzaicloud.spark.metrics.CollectorDecorator.FamilyBuilder
import com.banzaicloud.spark.metrics.PushTimestampDecorator.PushTimestampProvider
import com.codahale.metrics.{Counter, Gauge, Histogram, Meter, MetricFilter, MetricRegistry, Timer}
import io.prometheus.client.Collector.MetricFamilySamples
import io.prometheus.client.Collector.MetricFamilySamples.Sample
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.jmx.JmxCollector

import scala.collection.JavaConverters._
import scala.util.matching.Regex

object NameDecorator {
  case class Replace(Regex: Regex, replacement: String)
}

trait NameDecorator extends CollectorDecorator {
  val metricsNameReplace: Option[NameDecorator.Replace]

  protected override def familyBuilder = {
    super.familyBuilder.copy(
      familyName = mfs => replaceName(mfs.name),
      sampleBuilder = super.familyBuilder.sampleBuilder.copy(
        sampleName = s => replaceName(s.name)
      )
    )
  }

  private def replaceName(name: String) = {
    metricsNameReplace.map {
      case NameDecorator.Replace(regex, replacement) => regex.replaceAllIn(name, replacement)
    }.getOrElse(name)
  }
}

trait LabelsDecorator extends CollectorDecorator {
  val extraLabels: Map[String, String]

  private val labelNames = extraLabels.keys.toList.asJava
  private val labelValues = extraLabels.values.toList.asJava

  protected override def familyBuilder = {
    super.familyBuilder.copy(
      sampleBuilder = super.familyBuilder.sampleBuilder.copy(
        sampleLabelNames = s => mergeLists(s.labelNames, labelNames),
        sampleLabelValues = s => mergeLists(s.labelValues, labelValues)
      )
    )
  }

  private def mergeLists(list1: util.List[String], list2: util.List[String]): util.List[String] = {
    val newList = new util.ArrayList[String](list1)
    newList.addAll(list2)
    newList
  }
}

object PushTimestampDecorator {
  case class PushTimestampProvider(getTimestamp: () => Long = System.currentTimeMillis) extends AnyVal
}

trait PushTimestampDecorator extends CollectorDecorator {
  val maybeTimestampProvider: Option[PushTimestampProvider]

  protected override def map(source: util.List[MetricFamilySamples], builder: FamilyBuilder) = {
    val builderWithTimestamp = maybeTimestampProvider match {
      case Some(provider) =>
        val timestamp: java.lang.Long = provider.getTimestamp()
        builder.copy(
          sampleBuilder = builder.sampleBuilder.copy(
            sampleTimestamp = _ => timestamp
          )
        )
      case None => builder
    }
    super.map(source, builderWithTimestamp)
  }
}

trait ConstantHelpDecorator extends CollectorDecorator {
  val constantHelp: String

  protected override val familyBuilder = super.familyBuilder.copy(
    helpMessage = _ => constantHelp
  )
}

/**
 * +  * Wraps DropwizardExports but applies a MetricFilter each time we collect.
 * +  * This keeps it up-to-date as metrics are added/removed during runtime.
 * + */
private class FilteringDropwizardCollector(src: MetricRegistry, f: MetricFilter) extends io.prometheus.client.Collector {
  override def collect(): util.List[MetricFamilySamples] = {
    val snapshot = new MetricRegistry()

    src.getGauges(f).asScala.foreach { case (n, m: Gauge[_]) => snapshot.register(n, m) }
    src.getCounters(f).asScala.foreach { case (n, m: Counter) => snapshot.register(n, m) }
    src.getHistograms(f).asScala.foreach { case (n, m: Histogram) => snapshot.register(n, m) }
    src.getMeters(f).asScala.foreach { case (n, m: Meter) => snapshot.register(n, m) }
    src.getTimers(f).asScala.foreach { case (n, m: Timer) => snapshot.register(n, m) }

    new DropwizardExports(snapshot).collect()
  }
}

class SparkDropwizardExports(private val registry: MetricRegistry,
                             override val metricsNameReplace: Option[NameDecorator.Replace],
                             override val extraLabels: Map[String, String],
                             override val maybeTimestampProvider: Option[PushTimestampProvider],
                             val metricFilter: MetricFilter = MetricFilter.ALL)
  extends CollectorDecorator(new FilteringDropwizardCollector(registry, metricFilter))
    with NameDecorator
    with LabelsDecorator
    with PushTimestampDecorator
    with ConstantHelpDecorator {
  override val constantHelp: String = "Generated from Dropwizard metric import"
}

class SparkJmxExports(private val jmxCollector: JmxCollector,
                      override val extraLabels: Map[String, String],
                      override val maybeTimestampProvider: Option[PushTimestampProvider])
  extends CollectorDecorator(jmxCollector)
    with LabelsDecorator
    with PushTimestampDecorator