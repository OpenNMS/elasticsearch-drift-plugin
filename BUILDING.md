* Copy the DateHistogramAggregator and all of the related classes to a new package
* Rename to ProportionalSumAggregator
* Add the start and end fields to the builder, following the same pattern as the existing fields


# Intellij

When running tests using Intellij, change the `Shorted command line` option in the `Run/Debug Configurations` to use `JAR Manifest`, otherwise the tests will complain about "jar hell".
