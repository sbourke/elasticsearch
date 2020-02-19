package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.hamcrest.CoreMatchers.containsString;

public class RecallAtKTests extends ESTestCase {
    private static final int IRRELEVANT_RATING_0 = 0;
    private static final int RELEVANT_RATING_1 = 1;

    public void testRecallAtFiveCalculation() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(createRatedDoc("test", "0", IRRELEVANT_RATING_0));
        rated.add(createRatedDoc("test", "1", IRRELEVANT_RATING_0));
        rated.add(createRatedDoc("test", "2", RELEVANT_RATING_1));
        rated.add(createRatedDoc("test", "3", IRRELEVANT_RATING_0));
        rated.add(createRatedDoc("test", "4", IRRELEVANT_RATING_0));
        EvalQueryQuality evaluated = (new RecallAtK(1, false, 5)).evaluate("id", toSearchHits(rated, "test"), rated);
        assertEquals(1, evaluated.metricScore(), 0.00001);
        assertEquals(1, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(5, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testRecallAtFiveIgnoreOneResult() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(createRatedDoc("test", "0", IRRELEVANT_RATING_0));
        rated.add(createRatedDoc("test", "1", IRRELEVANT_RATING_0));
        rated.add(createRatedDoc("test", "2", IRRELEVANT_RATING_0));
        rated.add(createRatedDoc("test", "3", RELEVANT_RATING_1));
        rated.add(createRatedDoc("test", "4", IRRELEVANT_RATING_0));
        EvalQueryQuality evaluated = (new RecallAtK(1, false, 3)).evaluate("id", toSearchHits(rated, "test"), rated);
        assertEquals((double) 0, evaluated.metricScore(), 0.00001);
        assertEquals(0, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(3, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testIgnoreUnlabeled() {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(createRatedDoc("test", "0", RELEVANT_RATING_1));
        rated.add(createRatedDoc("test", "1", RELEVANT_RATING_1));
        // add an unlabeled search hit
        SearchHit[] searchHits = Arrays.copyOf(toSearchHits(rated, "test"), 3);
        searchHits[2] = new SearchHit(2, "2", Collections.emptyMap());
        searchHits[2].shard(new SearchShardTarget("testnode", new ShardId("index", "uuid", 0), null, OriginalIndices.NONE));

        EvalQueryQuality evaluated = (new RecallAtK()).evaluate("id", searchHits, rated);
        assertEquals((double) 2 / 2, evaluated.metricScore(), 0.00001);
        assertEquals(2, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(3, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRetrieved());

        // also try with setting `ignore_unlabeled`
        RecallAtK recall = new RecallAtK(1, true, 10);
        evaluated = recall.evaluate("id", searchHits, rated);
        assertEquals((double) 2 / 2, evaluated.metricScore(), 0.00001);
        assertEquals(2, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(2, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRetrieved());
    }

    private static RatedDocument createRatedDoc(String index, String id, int rating) {
        return new RatedDocument(index, id, rating);
    }

    public void testNoRatedDocs() throws Exception {
        SearchHit[] hits = new SearchHit[5];
        for (int i = 0; i < 5; i++) {
            hits[i] = new SearchHit(i, i + "", Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new ShardId("index", "uuid", 0), null, OriginalIndices.NONE));
        }
        EvalQueryQuality evaluated = (new RecallAtK()).evaluate("id", hits, Collections.emptyList());
        assertEquals(0.0d, evaluated.metricScore(), 0.00001);
        assertEquals(0, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(5, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRetrieved());

        // also try with setting `ignore_unlabeled`
        RecallAtK recall = new RecallAtK(1, true, 10);
        evaluated = recall.evaluate("id", hits, Collections.emptyList());
        assertEquals(0.0d, evaluated.metricScore(), 0.00001);
        assertEquals(0, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(0, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testNoResults() throws Exception {
        SearchHit[] hits = new SearchHit[0];
        EvalQueryQuality evaluated = (new RecallAtK()).evaluate("id", hits, Collections.emptyList());
        assertEquals(0.0d, evaluated.metricScore(), 0.00001);
        assertEquals(0, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRelevantRetrieved());
        assertEquals(0, ((RecallAtK.Detail) evaluated.getMetricDetails()).getRetrieved());
    }

    public void testParseFromXContent() throws IOException {
        String xContent = " {\n" + "   \"relevant_rating_threshold\" : 2" + "}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            RecallAtK recallAt = RecallAtK.fromXContent(parser);
            assertEquals(2, recallAt.getRelevantRatingThreshold());
        }
    }

    public void testCombine() {
        RecallAtK metric = new RecallAtK();
        List<EvalQueryQuality> partialResults = new ArrayList<>(3);
        partialResults.add(new EvalQueryQuality("a", 0.1));
        partialResults.add(new EvalQueryQuality("b", 0.2));
        partialResults.add(new EvalQueryQuality("c", 0.6));
        assertEquals(0.3, metric.combine(partialResults), Double.MIN_VALUE);
    }

    public void testInvalidRelevantThreshold() {
        expectThrows(IllegalArgumentException.class, () -> new RecallAtK(-1, false, 10));
    }

    public void testInvalidK() {
        expectThrows(IllegalArgumentException.class, () -> new RecallAtK(1, false, -10));
    }

    private static SearchHit[] toSearchHits(List<RatedDocument> rated, String index) {
        SearchHit[] hits = new SearchHit[rated.size()];
        for (int i = 0; i < rated.size(); i++) {
            hits[i] = new SearchHit(i, i + "", Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new ShardId(index, "uuid", 0), null, OriginalIndices.NONE));
        }
        return hits;
    }

    private static SearchHit[] randomHits(List<RatedDocument> rated, String index, int numberOfResults) {
        SearchHit[] hits = new SearchHit[numberOfResults];
        for (int i = 0; i < numberOfResults; i++) {
            hits[i] = new SearchHit(i, i + "", Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new ShardId(index, "uuid", 0), null, OriginalIndices.NONE));
        }
        return hits;
    }

    public static RecallAtK createTestItem() {
        return new RecallAtK(randomIntBetween(0, 10), randomBoolean(), randomIntBetween(1, 50));
    }

    public void testXContentRoundtrip() throws IOException {
        RecallAtK testItem = createTestItem();
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        XContentBuilder shuffled = shuffleXContent(testItem.toXContent(builder, ToXContent.EMPTY_PARAMS));
        try (XContentParser itemParser = createParser(shuffled)) {
            itemParser.nextToken();
            itemParser.nextToken();
            RecallAtK parsedItem = RecallAtK.fromXContent(itemParser);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    public void testXContentParsingIsNotLenient() throws IOException {
        RecallAtK testItem = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(testItem, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, null, random());
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            parser.nextToken();
            parser.nextToken();
            XContentParseException exception = expectThrows(XContentParseException.class, () -> RecallAtK.fromXContent(parser));
            assertThat(exception.getMessage(), containsString("[recall] unknown field"));
        }
    }

    public void testSerialization() throws IOException {
        RecallAtK original = createTestItem();
        RecallAtK deserialized = ESTestCase.copyWriteable(original, new NamedWriteableRegistry(Collections.emptyList()),
            RecallAtK::new);
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testEqualsAndHash() throws IOException {
        checkEqualsAndHashCode(createTestItem(), RecallAtKTests::copy, RecallAtKTests::mutate);
    }

    private static RecallAtK copy(RecallAtK original) {
        return new RecallAtK(original.getRelevantRatingThreshold(), original.getIgnoreUnlabeled(),
            original.forcedSearchSize().getAsInt());
    }

    private static RecallAtK mutate(RecallAtK original) {
        RecallAtK rAtK;
        switch (randomIntBetween(0, 2)) {
            case 0:
                rAtK = new RecallAtK(original.getRelevantRatingThreshold(), !original.getIgnoreUnlabeled(),
                    original.forcedSearchSize().getAsInt());
                break;
            case 1:
                rAtK = new RecallAtK(randomValueOtherThan(original.getRelevantRatingThreshold(), () -> randomIntBetween(0, 10)),
                    original.getIgnoreUnlabeled(), original.forcedSearchSize().getAsInt());
                break;
            case 2:
                rAtK = new RecallAtK(original.getRelevantRatingThreshold(),
                    original.getIgnoreUnlabeled(), original.forcedSearchSize().getAsInt() + 1);
                break;
            default:
                throw new IllegalStateException("The test should only allow three parameters mutated");
        }
        return rAtK;
    }

}
