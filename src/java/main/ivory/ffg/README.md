Ivory - Fast Feature Generation
====================

Adding new DocumentVector classes
---------------------------------
To add a new DocumentVector implementation, you must implement the `ivory.ffg.data.DocumentVector` interface. Additionally,
you need to modify `ivory.ffg.data.DocumentVectorUtility` in order for your implementation to be compatible with the
framework.

Adding new Feature/ScoringFunction classes
--------------------------
To add a new Feature implementation, you must implement the `ivory.ffg.feature.Feature` interface. Additionally, `ivory.ffg.util.FeatureUtility` must be updated in order for the framework to parse the new feature.

Similary, a scoring function must implement `ivory.ffg.score.ScoringFunction`. Also, `ivory.ffg.util.FeatureUtility` must be updated to parse the new scoring function.

