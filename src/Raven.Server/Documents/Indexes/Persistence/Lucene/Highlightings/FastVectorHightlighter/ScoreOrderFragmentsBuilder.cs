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
using System.Collections.Generic;
using WeightedFragInfo = Lucene.Net.Search.Vectorhighlight.FieldFragList.WeightedFragInfo;

namespace Lucene.Net.Search.Vectorhighlight
{
    /*
 * An implementation of FragmentsBuilder that outputs score-order fragments.
 */
    public sealed class ScoreOrderFragmentsBuilder : BaseFragmentsBuilder
    {

        /// <summary>
        /// a constructor.
        /// </summary>
        public ScoreOrderFragmentsBuilder():base()
        {
        }


        /// <summary>
        /// a constructor.
        /// </summary>
        /// <param name="preTags">array of pre-tags for markup terms</param>
        /// <param name="postTags">array of post-tags for markup terms</param>
        public ScoreOrderFragmentsBuilder(String[] preTags, String[] postTags):  base(preTags, postTags)
        {
        }

        /// <summary>
        /// Sort by score the list of WeightedFragInfo
        /// </summary>
        public override List<WeightedFragInfo> GetWeightedFragInfoList(List<WeightedFragInfo> src)
        {
            src.Sort(new ScoreComparator());
            return src;
        }

        public sealed class ScoreComparator : IComparer<WeightedFragInfo>
        {  // Comparator<WeightedFragInfo> {

            public int Compare(WeightedFragInfo o1, WeightedFragInfo o2)
            {
                if (o1.totalBoost > o2.totalBoost) return -1;
                else if (o1.totalBoost < o2.totalBoost) return 1;
                // if same score then check startOffset
                else
                {
                    if (o1.startOffset < o2.startOffset) return -1;
                    else if (o1.startOffset > o2.startOffset) return 1;
                }
                return 0;
            }
        }
    }

}
