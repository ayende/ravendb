/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using Lucene.Net.Index;
using Lucene.Net.Store;

namespace Lucene.Net.Search.Vectorhighlight
{
    public sealed class FastVectorHighlighter
    {

        public static bool DEFAULT_PHRASE_HIGHLIGHT = true;
        public static bool DEFAULT_FIELD_MATCH = true;
        private bool phraseHighlight;
        private bool fieldMatch;
        private FragListBuilder fragListBuilder;
        private FragmentsBuilder fragmentsBuilder;
        private int phraseLimit = Int32.MaxValue;

        /// <summary>
        /// the default constructor.
        /// </summary>
        public FastVectorHighlighter():this(DEFAULT_PHRASE_HIGHLIGHT, DEFAULT_FIELD_MATCH)
        {
        }

        /// <summary>
        /// a constructor. Using SimpleFragListBuilder and ScoreOrderFragmentsBuilder. 
        /// </summary>
        /// <param name="phraseHighlight">true or false for phrase highlighting</param>
        /// <param name="fieldMatch">true of false for field matching</param>
        public FastVectorHighlighter(bool phraseHighlight, bool fieldMatch):this(phraseHighlight, fieldMatch, new SimpleFragListBuilder(), new ScoreOrderFragmentsBuilder())
        {
        }

        /// <summary>
        /// a constructor. A FragListBuilder and a FragmentsBuilder can be specified (plugins).
        /// </summary>
        /// <param name="phraseHighlight">true of false for phrase highlighting</param>
        /// <param name="fieldMatch">true of false for field matching</param>
        /// <param name="fragListBuilder">an instance of FragListBuilder</param>
        /// <param name="fragmentsBuilder">an instance of FragmentsBuilder</param>
        public FastVectorHighlighter(bool phraseHighlight, bool fieldMatch,
            FragListBuilder fragListBuilder, FragmentsBuilder fragmentsBuilder)
        {
            this.phraseHighlight = phraseHighlight;
            this.fieldMatch = fieldMatch;
            this.fragListBuilder = fragListBuilder;
            this.fragmentsBuilder = fragmentsBuilder;
        }

        /// <summary>
        /// create a FieldQuery object. 
        /// </summary>
        /// <param name="query">a query</param>
        /// <returns>the created FieldQuery object</returns>
        public FieldQuery GetFieldQuery(Query query)
        {
            return new FieldQuery(query, phraseHighlight, fieldMatch);
        }


        /// <summary>
        /// return the best fragments.
        /// </summary>
        /// <param name="fieldQuery">FieldQuery object</param>
        /// <param name="reader">IndexReader of the index</param>
        /// <param name="docId">document id to be highlighted</param>
        /// <param name="fieldName">field of the document to be highlighted</param>
        /// <param name="fragCharSize">the length (number of chars) of a fragment</param>
        /// <param name="maxNumFragments">maximum number of fragments</param>
        /// <param name="fragCharSize1"></param>
        /// <returns>created fragments or null when no fragments created. Size of the array can be less than maxNumFragments</returns>
        public string[] GetBestFragments(FieldQuery fieldQuery, IndexReader reader, int docId, string fieldName, int fragCharSize, int maxNumFragments, IState state)
        {
            FieldFragList fieldFragList = GetFieldFragList(fieldQuery, reader, docId, fieldName, fragCharSize, state);
            return fragmentsBuilder.CreateFragments(reader, docId, fieldName, fieldFragList, maxNumFragments, fragCharSize, state);
        }

        private FieldFragList GetFieldFragList(FieldQuery fieldQuery, IndexReader reader, int docId,
            String fieldName, int fragCharSize, IState state)
        {
            FieldTermStack fieldTermStack = new FieldTermStack(reader, docId, fieldName, fieldQuery, state);
            FieldPhraseList fieldPhraseList = new FieldPhraseList(fieldTermStack, fieldQuery, phraseLimit);
            return fragListBuilder.CreateFieldFragList(fieldPhraseList, fragCharSize);
        }
    }
}
