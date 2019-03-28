using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Raven.Client.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TestingEnvironment.Client;

namespace FacetedSearch
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("On");
            Thread.Sleep(3000);
            Console.WriteLine("Starting");

            using (var client = new FacetedSearchTest(args[0]))
            {
                client.Initialize();
                client.RunActualTest();
            }
        }
    }
    

    public class FacetedSearchTest : BaseTest
    {
        private const int InitialDocsCount = 100;
        private List<string> generatedIDs = new List<string>();

        string collection = "cups"+Guid.NewGuid().ToString().Replace('-','x');
        public class Cup
        {
            public long Volume;
            public List<string> Features;
        }
        
        
        public FacetedSearchTest(string orchestratorUrl):base(orchestratorUrl, nameof(FacetedSearchTest),"Maxim Buryak")
        {

        }

        private void InsertCups(int count)
        {
            var features = Enumerable.Range(0, 1000).Select(x => x.ToString()).ToList();
            using (var bi = DocumentStore.BulkInsert())
            {
                for (var i=0; i< count; i++)
                {
                    var metadata = new MetadataAsDictionary();
                    metadata[Constants.Documents.Metadata.Collection] = collection;

                    string id = $"{collection}/{generatedIDs.Count}";
                    bi.Store(new Cup
                    {
                        Volume = i,
                        Features = features
                    }, id, metadata);
                    generatedIDs.Add(id);
                }
            }
        }

        public override void RunActualTest()
        {
            using (DocumentStore)
            {
                var indexDef = new IndexDefinition();

                indexDef.Name = "CupsFacets" + collection;
                indexDef.Maps = new HashSet<string>
                {
                    "from cup in docs."+ collection + " select new { " +
                    "_ = cup.Features.Select(x=>CreateField('f' + x.ToString(),cup.Volume,true,true)) " +
                    "}"
                };
                
                indexDef.LockMode = IndexLockMode.LockedIgnore;

                DocumentStore.Maintenance.Send(new PutIndexesOperation(indexDef));

                ReportEvent(new TestingEnvironment.Common.EventInfo
                {
                    Message = $"StartingInserts for collection {collection}",
                    Type = TestingEnvironment.Common.EventInfo.EventType.Info
                });

                InsertCups(InitialDocsCount);


                var facets = new List<RangeFacet>();

                for (var i = 0; i < 1000; i++)
                {
                    facets.Add(new RangeFacet
                    {
                        DisplayFieldName = $"f{i}",
                        Ranges = new List<string>
                        {
                            $"f{i} >= 0 and f{i} < 10",
                            $"f{i} >= 10 and f{i} < 100",
                            $"f{i} >= 100 and f{i} < 1000",
                            $"f{i} >= 100 and f{i} < 10000",
                            $"f{i} >= 10000 and f{i} < 1000000",
                        },
                        Options = new FacetOptions
                        {
                            TermSortMode = FacetTermSortMode.CountDesc
                        }
                    });
                }

                using (var session = DocumentStore.OpenSession())
                {
                    session.Store(new FacetSetup { Id = "facets/" + collection, RangeFacets = facets });
                    session.SaveChanges();
                }

                ReportEvent(new TestingEnvironment.Common.EventInfo
                {
                    Message = "Stored facets doc",
                    Type = TestingEnvironment.Common.EventInfo.EventType.Info
                });

                ReportEvent(new TestingEnvironment.Common.EventInfo
                {
                    Message = "StartingQueries",
                    Type = TestingEnvironment.Common.EventInfo.EventType.Info                    
                });

                for (int i = 0; i < 100; i++)
                {
                    InsertCups(1);
                    ReportEvent(new TestingEnvironment.Common.EventInfo
                    {
                        Message = "Stored a doc",
                        Type = TestingEnvironment.Common.EventInfo.EventType.Info
                    });
                    using (var session = DocumentStore.OpenSession())
                    {
                        var results = session.Query<Cup>(indexDef.Name).Customize(x => x.WaitForNonStaleResults())
                            .AggregateUsing("facets/" + collection).Execute();

                        foreach (var facet in facets)
                        {
                            if (results.TryGetValue(facet.DisplayFieldName, out var facetResult) == false)
                            {
                                ReportEvent(new TestingEnvironment.Common.EventInfo
                                {
                                    Message = $"Assertion failed in {i}th repeated faceted query, facet {facet.DisplayFieldName} not found",
                                    Type = TestingEnvironment.Common.EventInfo.EventType.TestFailure
                                });
                                return;
                            }

                            if (facetResult.Values.Count != 5)
                            {
                                ReportEvent(new TestingEnvironment.Common.EventInfo
                                {
                                    Message = $"Assertion failed in {i}th repeated faceted query, there were not enough ranges for facet {facet.DisplayFieldName}",
                                    Type = TestingEnvironment.Common.EventInfo.EventType.TestFailure
                                });
                                return;
                            }

                            for (var j=0; j< facet.Ranges.Count&& j < (int)Math.Log10(InitialDocsCount); j++)
                            {
                                if (facetResult.Values[j].Range != facet.Ranges[j])
                                {
                                    ReportEvent(new TestingEnvironment.Common.EventInfo
                                    {
                                        Message = $"Assertion failed in {i}th repeated faceted query, the expected range was {facet.Ranges[j]} but got {facetResult.Values[j].Range} for facet {facet.DisplayFieldName}",
                                        Type = TestingEnvironment.Common.EventInfo.EventType.TestFailure
                                    });
                                    return;
                                }

                                var iterationAddition = i + 1;                                

                                if ((int)Math.Log10(i+1) != (int)Math.Log10(j+1))
                                    iterationAddition =0;

                                int expectedCount = (int)Math.Pow(10, j+1) + iterationAddition;

                                for (var k=1; k<= j; k++)
                                {
                                    expectedCount -= (int)Math.Pow(10, k);
                                }
                                if ((int)Math.Log10(InitialDocsCount) >= (int)Math.Log10(expectedCount) && 
                                    facetResult.Values[j].Count != expectedCount)
                                {                                    
                                    ReportEvent(new TestingEnvironment.Common.EventInfo
                                    {
                                        Message = $"Assertion failed in {i}th repeated faceted query, the expected count for range {facet.Ranges[j]} was {expectedCount} but was {facetResult.Values[j].Count}",
                                        Type = TestingEnvironment.Common.EventInfo.EventType.TestFailure
                                    });
                                    return;
                                }
                            }
                        }
                    }
                    ReportEvent(new TestingEnvironment.Common.EventInfo
                    {
                        Message = "Performed faceted query",
                        Type = TestingEnvironment.Common.EventInfo.EventType.Info
                    });

                }

                ReportEvent(new TestingEnvironment.Common.EventInfo
                {
                    Message = "Great Success!",
                    Type = TestingEnvironment.Common.EventInfo.EventType.TestSuccess
                });
            }
        }
    }
}
