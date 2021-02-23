# Development Notes

## Modules

Maintain modules for every supported patch version of Elasticsearch i.e.:
* es-7.6.2
* es-7.10.2


## Releasing

Versioning uses Maven's revision mechanism (cf. https://maven.apache.org/maven-ci-friendly.html).

Tag the release and push. Tag names must have the form:

```
v<major>.<minor>.<patch>_<es-modules>
```

Where `es-modules` is a comma separated list of the modules that should be released. E.g. in order to release version `1.2.0` of the modules `es-7.6.2` and `es-7.10.2` the following tag has to be created:

```
v1.2.0_es-7.6.2,es-7.10.2
```

Tagging (and thereby releasing) can also be done for each module separately. 

The build pipeline with automatically publish the artifacts to GitHub, Maven Central and Packagecloud.

## Misc.

### Initial build of the plugin

Plugin was based on the DateHistogramAggregator

* Copy the DateHistogramAggregator and all of the related classes to a new package
* Rename to ProportionalSumAggregator
* Add the start and end fields to the builder, following the same pattern as the existing fields


### Intellij

When running tests using Intellij, change the `Shorted command line` option in the `Run/Debug Configurations` to use `JAR Manifest`, otherwise the tests will complain about "jar hell".

