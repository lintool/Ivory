Ivory - Fast Feature Generation
====================

Queries
------
Queries must be in the following XML format:

	<parameters>
	<query id="query_id">query_text</query>
	</parameters>

Documents
--------
Documents must be tab-delimited as follows:

	<line> .=. <query-id> \t <document-id>
	<query-id> .=. <integer>
	<document-id> .=. <integer>

Features
--------
Features must be in the following XML format:

	<parameters>
	<feature id="FeatureID" \
		featureClass="FeatureClass" \
		scoringFunctionClass="ScoringFunctionClass" \
		<parameter>:"<values>" />
	</parameters>

For a sample feature file see: `data/ivory/ffg/features.xml`.

Preparing document vectors
--------------------------
As the first step, you need to pack documents into one of the various document vector data structures located in the package `ivory.ffg.data`.

	java -cp 'lib/*:dist/ivory-private-0.0.1.jar' ivory.ffg.preprocessing.GenerateDocumentVectors \
	-index <index-root-path> -dvclass <DocumentVector-class> -judgment <tab-delimited-document-path> \
	-output <output-path>

To Prepare a positional inverted index, you need to run the following command:

	java -cp 'lib/*:dist/ivory-private-0.0.1.jar' ivory.ffg.preprocessing.GenerateCompressedPositionalPostings \
	-index <index-root-path> -query <query-path> -spam <spam-percentile-scores> -output <output-path>


Extracting features
-------------------
Given a feature file, a set of queries and a set of tab-delimited documents, you can run the feature extractors using any of the drivers provided in the `ivory.ffg.driver` package. For example, to run the feature extractor using the on-the-fly indexing, use the following command:

	java -cp 'lib/*:dist/ivory-private-0.0.1.jar' ivory.ffg.driver.DocumentVectorOnTheFlyIndexing \
	-index <index-path> -dvclass ivory.ffg.data.DocumentVectorHashedArray -document <document-vector-path> \
	-query <query-path> -judgment <tab-delimited-document-path> -feature <feature-path>

Or use the following to retrieve candidates using SmallAdaptive and extract their features:

	java -cp 'lib/*:dist/ivory-private-0.0.1.jar' ivory.ffg.driver.RankAndFeaturesSmallAdaptive \
	-index <index-root-path> -posting <compressedPositionalPostings-root-path> \
	-query <query-path> -judgment <tab-delimited-document-path> -feature <feature-path> [-hits <hits>]

The default value of `hits` is 10,000 documents.

Adding new DocumentVector classes
---------------------------------
To add a new DocumentVector implementation, you must implement the `ivory.ffg.data.DocumentVector` interface. Additionally,
you need to modify `ivory.ffg.data.DocumentVectorUtility` in order for your implementation to be compatible with the
framework.

Adding new Feature/ScoringFunction classes
--------------------------
To add a new Feature implementation, you must implement the `ivory.ffg.feature.Feature` interface. Additionally, `ivory.ffg.util.FeatureUtility` must be updated in order for the framework to parse the new feature.

Similary, a scoring function must implement `ivory.ffg.score.ScoringFunction`. Also, `ivory.ffg.util.FeatureUtility` must be updated to parse the new scoring function.

