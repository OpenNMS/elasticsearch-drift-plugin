# Development Notes

## Branches

Maintain branches for every supported minor version of Elasticsearch i.e.:
* es-6.5.x
* es-6.6.x
* es-6.7.x

Versions in the pom.xml for these branches should be 6.5.Y-SNAPSHOT, where Y is the last patch release for which the plugin was built.

## Releasing

When releasing, change version to match Elasticsearch version i.e.:
```
mvn versions:set -DnewVersion=6.5.2
```

Then update the `rpmRelease` property to be `1`.

Tag the release and push.

The build pipeline with automatically publish the artifacts to GitHub, Maven Central and Packagecloud.

## Misc.

### Initial build of the plugin

Plugin was based on the DateHistogramAggregator

* Copy the DateHistogramAggregator and all of the related classes to a new package
* Rename to ProportionalSumAggregator
* Add the start and end fields to the builder, following the same pattern as the existing fields


### Intellij

When running tests using Intellij, change the `Shorted command line` option in the `Run/Debug Configurations` to use `JAR Manifest`, otherwise the tests will complain about "jar hell".

