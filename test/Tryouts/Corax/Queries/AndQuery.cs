﻿using Tryouts.Corax.Queries;

namespace Tryouts.Corax
{
    public class AndQuery : Query
    {
        private readonly Query _left, _right;

        public AndQuery(IndexReader reader, Query left, Query right) : base(reader)
        {
            _left = left;
            _right = right;
        }


        public override void Run(out PackedBitmapReader results)
        {
            _left.Run(out var leftResults);
            try
            {
                _right.Run(out var rightResults);
                try
                {
                    PackedBitmapReader.And(Context, ref leftResults, ref rightResults, out results);
                }
                finally
                {
                    rightResults.Dispose();
                }
            }
            finally
            {
                leftResults.Dispose();
            }
        }
    }
}
