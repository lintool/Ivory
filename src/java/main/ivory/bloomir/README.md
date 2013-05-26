Ivory - Bloom IR
====================

Queries
------
Queries must be in the following XML format:

	<parameters>
	<query id="query_id">query_text</query>
	</parameters>

Pre-processing postings lists
--------------------------
As the first step, you will need to convert the non-positional postings lists to `ivory.bloomir.data.CompressedPostings`.
To do so, you can run the following command:

	java -cp 'lib/*:dist/ivory-private-0.0.1.jar' ivory.bloomir.preprocessing.GenerateCompressedPostings \
	-index <ivory-index-root-path> -spam <spam-percentile-scores> -output <output-root-path>

To generate Bloom filters use the following driver:

	java -cp 'lib/*:dist/ivory-private-0.0.1.jar' ivory.bloomir.preprocessing.GenerateBloomFilters \
	-index <ivory-index-root-path> -output <output-root-path> -spam <spam-percentile-scores>
	-bpe <bits-per-element> -nbHash <number-of-Hash-functions>


Rankers
-------------------
To run the Bloom ranker:

	java -cp 'lib/*:dist/ivory-private-0.0.1.jar' ivory.bloomir.ranker.BloomRanker \
	-index <ivory-index-root-path> -posting <compressedPostings-root-path> -bloom <bloomFilters-root-path> \
	-query <query-path> [-output <output-path> -spam <spam-percentile-scores>] [-hits <hits>]

To run the SmallAdaptive or LinearMerging rankers:

	java -cp 'lib/*:dist/ivory-private-0.0.1.jar' ivory.bloomir.ranker.[SmallAdaptiveRanker/LinearMergingRanker] \
	-index <ivory-index-root-path> -posting <compressedPostings-root-path> \
	-query <query-path> [-output <output-path> -spam <spam-percentile-scores>] [-hits <hits>]

Pleae note that `spam` is a required option if `output` is provided. Also, the default value of `hits` is 10,000 document.
