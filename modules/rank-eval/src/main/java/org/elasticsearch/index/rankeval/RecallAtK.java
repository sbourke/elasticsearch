/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.index.rankeval.EvaluationMetric.joinHitsWithRatings;

/**
 * Metric implementing Recall@K
 * (https://en.wikipedia.org/wiki/Evaluation_measures_(information_retrieval)#Recall).<br>
 * By default documents with a rating equal or bigger than 1 are considered to
 * be "relevant" for this calculation. This value can be changes using the
 * relevant_rating_threshold` parameter.<br>
 * The `ignore_unlabeled` parameter (default to false) controls if unrated
 * documents should be ignored.
 * The `k` parameter (defaults to 10) controls the search window size.
 */
public class RecallAtK implements EvaluationMetric {

    public static final String NAME = "recall";

    private static final ParseField RELEVANT_RATING_FIELD = new ParseField("relevant_rating_threshold");
    private static final ParseField IGNORE_UNLABELED_FIELD = new ParseField("ignore_unlabeled");
    private static final ParseField K_FIELD = new ParseField("k");

    private static final int DEFAULT_K = 10;

    private final boolean ignoreUnlabeled;
    private final int relevantRatingThreshhold;
    private final int k;

    /**
     * Metric implementing Recall@K.
     * @param threshold
     *            ratings equal or above this value will be considered relevant.
     * @param ignoreUnlabeled
     *            Controls how unlabeled documents in the search hits are treated.
     *            Set to 'true', unlabeled documents are ignored and neither count
     *            as true or false positives. Set to 'false', they are treated as
     *            false positives.
     * @param k
     *            controls the window size for the search results the metric takes into account
     */
    public RecallAtK(int threshold, boolean ignoreUnlabeled, int k) {
        if (threshold < 0) {
            throw new IllegalArgumentException("Relevant rating threshold for recall must be positive integer.");
        }
        if (k <= 0) {
            throw new IllegalArgumentException("Window size k must be positive.");
        }
        this.relevantRatingThreshhold = threshold;
        this.ignoreUnlabeled = ignoreUnlabeled;
        this.k = k;
    }

    public RecallAtK() {
        this(1, false, DEFAULT_K);
    }

    private static final ConstructingObjectParser<RecallAtK, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> {
                Integer threshHold = (Integer) args[0];
                Boolean ignoreUnlabeled = (Boolean) args[1];
                Integer k = (Integer) args[2];
                return new RecallAtK(threshHold == null ? 1 : threshHold,
                        ignoreUnlabeled == null ? false : ignoreUnlabeled,
                                k == null ? DEFAULT_K : k);
            });

    static {
        PARSER.declareInt(optionalConstructorArg(), RELEVANT_RATING_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), IGNORE_UNLABELED_FIELD);
        PARSER.declareInt(optionalConstructorArg(), K_FIELD);
    }

    RecallAtK(StreamInput in) throws IOException {
        relevantRatingThreshhold = in.readVInt();
        ignoreUnlabeled = in.readBoolean();
        k = in.readVInt();
    }

    int getK() {
        return this.k;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        System.out.println(relevantRatingThreshhold+","+ignoreUnlabeled+","+k+" kaslfdjkalsdfjalsdkjfkalsjd ");
        out.writeVInt(relevantRatingThreshhold);
        out.writeBoolean(ignoreUnlabeled);
        out.writeVInt(k);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Return the rating threshold above which ratings are considered to be
     * "relevant" for this metric. Defaults to 1.
     */
    public int getRelevantRatingThreshold() {
        return relevantRatingThreshhold;
    }

    /**
     * Gets the 'ignore_unlabeled' parameter.
     */
    public boolean getIgnoreUnlabeled() {
        return ignoreUnlabeled;
    }

    @Override
    public OptionalInt forcedSearchSize() {
        return OptionalInt.of(k);
    }

    public static RecallAtK fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Compute recallAtN based on provided relevant document IDs.
     *
     * @return recall at n for above {@link SearchResult} list.
     **/
    @Override
    public EvalQueryQuality evaluate(String taskId, SearchHit[] hits,
            List<RatedDocument> ratedDocs) {
        int truePositives = 0;
        int falsePositives = 0;
         List<RatedSearchHit> ratedSearchHits = joinHitsWithRatings(hits, ratedDocs);
        int relevantDocs = ratedDocs.size();
        int numberOfRelevantDocs = ratedSearchHits.size();
        for (RatedSearchHit hit : ratedSearchHits) {
            OptionalInt rating = hit.getRating();
            if (rating.isPresent()) {
                if (rating.getAsInt() >= this.relevantRatingThreshhold) {
                    truePositives++;
                } else {
                    falsePositives++;
                }
            } else if (ignoreUnlabeled == false) {
                falsePositives++;
            }
        }
        double recall = 0.0;
        if (truePositives  > 0 && relevantDocs > 0) {
            recall = (double) truePositives / relevantDocs;
        }
        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(taskId, recall);
        evalQueryQuality.setMetricDetails(
                new RecallAtK.Detail(truePositives, truePositives + falsePositives));
        evalQueryQuality.addHitsAndRatings(ratedSearchHits);
        return evalQueryQuality;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(RELEVANT_RATING_FIELD.getPreferredName(), this.relevantRatingThreshhold);
        builder.field(IGNORE_UNLABELED_FIELD.getPreferredName(), this.ignoreUnlabeled);
        builder.field(K_FIELD.getPreferredName(), this.k);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RecallAtK other = (RecallAtK) obj;
        return Objects.equals(relevantRatingThreshhold, other.relevantRatingThreshhold)
                && Objects.equals(k, other.k)
                && Objects.equals(ignoreUnlabeled, other.ignoreUnlabeled);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(relevantRatingThreshhold, ignoreUnlabeled, k);
    }

    public static final class Detail implements MetricDetail {

        private static final ParseField DOCS_RETRIEVED_FIELD = new ParseField("docs_retrieved");
        private static final ParseField RELEVANT_DOCS_RETRIEVED_FIELD = new ParseField("relevant_docs_retrieved");
        private int relevantRetrieved;
        private int retrieved;

        Detail(int relevantRetrieved, int retrieved) {
            this.relevantRetrieved = relevantRetrieved;
            this.retrieved = retrieved;
        }

        Detail(StreamInput in) throws IOException {
            this.relevantRetrieved = in.readVInt();
            this.retrieved = in.readVInt();
        }

        @Override
        public XContentBuilder innerToXContent(XContentBuilder builder, Params params)
                throws IOException {
            builder.field(RELEVANT_DOCS_RETRIEVED_FIELD.getPreferredName(), relevantRetrieved);
            builder.field(DOCS_RETRIEVED_FIELD.getPreferredName(), retrieved);
            return builder;
        }

        private static final ConstructingObjectParser<Detail, Void> PARSER = new ConstructingObjectParser<>(NAME, true, args -> {
            return new Detail((Integer) args[0], (Integer) args[1]);
        });

        static {
            PARSER.declareInt(constructorArg(), RELEVANT_DOCS_RETRIEVED_FIELD);
            PARSER.declareInt(constructorArg(), DOCS_RETRIEVED_FIELD);
        }

        public static Detail fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(relevantRetrieved);
            out.writeVInt(retrieved);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        public int getRelevantRetrieved() {
            return relevantRetrieved;
        }

        public int getRetrieved() {
            return retrieved;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            RecallAtK.Detail other = (RecallAtK.Detail) obj;
            return Objects.equals(relevantRetrieved, other.relevantRetrieved)
                    && Objects.equals(retrieved, other.retrieved);
        }

        @Override
        public int hashCode() {
            return Objects.hash(relevantRetrieved, retrieved);
        }
    }
}
