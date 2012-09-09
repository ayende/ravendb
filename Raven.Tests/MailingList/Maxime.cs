using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq.Indexing;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class Maxime : RavenTest
    {
        [Fact]
        public void Spatial_Search_Should_Integrate_Distance_As_A_Boost_Factor()
        {
            var store = new EmbeddableDocumentStore { RunInMemory = true }.Initialize();
            store.ExecuteIndex(new SpatialIndex());

            using (var session = store.OpenSession())
            {
                session.Store(new SpatialEntity(45.70955, -73.569131)
                {
                    Id = "se/1",
                    Name = "Universite du Quebec a Montreal",
                    Description = "UQAM",
                });

                session.Store(new SpatialEntity(45.50955, -73.569131)
                {
                    Id = "se/2",
                    Name = "UQAM",
                    Description = "Universite du Quebec a Montreal",
                });

                session.Store(new SpatialEntity(45.60955, -73.569131)
                {
                    Id = "se/3",
                    Name = "UQAM",
                    Description = "Universite du Quebec a Montreal",
                });

                session.SaveChanges();
            }

            WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                //var ctx = SpatialContext.GEO_KM;
                //var shape = ctx.MakeCircle(ctx.MakePoint(-73.569131, 45.50955), 500);
                //var circle = ctx.ToString(shape);

                //var lon = DistanceUtils.NormLonDEG(45.50955);
                //var lng = DistanceUtils.NormLatDEG(-73.569131);

                var results = session.Advanced.LuceneQuery<SpatialEntity>("SpatialIndex")
                    .Where("Name: UQAM OR Description: UQAM")
                    //.RelatesToShape(Constants.DefaultSpatialFieldName, circle, SpatialRelation.Within)
                    .WithinRadiusOf(500, 45.50955, -73.569133)
                    //.SortByDistance()
                    .ToList();

                Assert.True(results[0].Id == "se/2");
                Assert.True(results[1].Id == "se/3");
                Assert.True(results[2].Id == "se/1");
            }

            store.Dispose();
        }

        public class SpatialIndex : AbstractIndexCreationTask<SpatialEntity>
        {
            public SpatialIndex()
            {
                Map =
                    entities =>
                    from e in entities
                    select new
                    {
                        Name = e.Name.Boost(3),
                        e.Description,
                        _ = SpatialGenerate(e.Latitude, e.Longitude)
                        //_ = SpatialGenerate(Constants.DefaultSpatialFieldName, e.Location, SpatialSearchStrategy.QuadPrefixTree, 24)
                    };

                Index(e => e.Name, FieldIndexing.Analyzed);
                Index(e => e.Description, FieldIndexing.Analyzed);
            }
        }

        public class SpatialEntity
        {
            public SpatialEntity() { }

            public SpatialEntity(double latitude, double longitude)
            {
                Latitude = latitude;
                Longitude = longitude;

                //var ctx = SpatialContext.GEO_KM;
                //var shape = ctx.MakePoint(longitude, latitude);

                //Location = ctx.ToString(shape);
            }

            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
            public double Latitude { get; set; }
            public double Longitude { get; set; }
            //public string Location { get; set; }
        }
    }
}