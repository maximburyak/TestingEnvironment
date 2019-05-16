﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Polly;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Client.ServerWide.Operations;
using TestingEnvironment.Client;
using TestingEnvironment.Common;

namespace CorruptedCasino
{
    public class CasinoTest : BaseTest
    {
        public CasinoTest(string orchestratorUrl, string testName) : base(orchestratorUrl, testName, "Karmel")
        {
           
        }

        public override void Initialize()
        {
            base.Initialize();

            try
            {
                new BetsIndex().Execute(DocumentStore);
            }
            catch
            {
                // ignore
            }

            // ConfigureExpiration
            DocumentStore.Maintenance.Send(new ConfigureExpirationOperation(new ExpirationConfiguration
            {
                Disabled = false,
                DeleteFrequencyInSec = 600
            }));

            // ConfigureRevisions
            var config = new RevisionsConfiguration
            {
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration
                    {
                        PurgeOnDelete = true,
                        MinimumRevisionAgeToKeep = TimeSpan.FromSeconds(180)
                    },
                    ["Bets"] = new RevisionsCollectionConfiguration()
                }
            };

            DocumentStore.Maintenance.Send(new ConfigureRevisionsOperation(config));
        }

        public IAsyncDocumentSession GetClusterSessionAsync => DocumentStore.OpenAsyncSession(new SessionOptions
        {
            TransactionMode = TransactionMode.ClusterWide
        });

        public IDocumentStore GetDocumentStore => DocumentStore;

        public IAsyncDocumentSession GetSessionAsync => DocumentStore.OpenAsyncSession();

        public class BetsIndex : AbstractIndexCreationTask<Bet, BetsIndex.BetsResult>
        {
            public class BetsResult
            {
                public string LotteryId;

                public long Total;

                public bool Won;

                public string[] BetsId;
            }

            public BetsIndex()
            {
                Map = bets => from bet in bets
                    let lottery = LoadDocument<Lottery>(bet.LotteryId)
                    where bet.BetStatus == Bet.Status.Active
                    select new
                    {
                        bet.LotteryId,
                        Total = bet.Price,
                        Won = lottery != null && lottery.Status == Lottery.LotteryStatus.Over && lottery.Result.OrderBy(x => x).SequenceEqual(bet.Numbers.OrderBy(x => x)),
                        BetsId = new[] { bet.Id }
                    };

                Reduce = results => from result in results
                    group result by new { result.LotteryId, result.Won } into g
                    select new
                    {
#pragma warning disable IDE0037 // Use inferred member name
                        LotteryId = g.Key.LotteryId,
                        Won = g.Key.Won,
                        Total = g.Sum(x => x.Total),
                        BetsId = g.Key.Won ? g.Select(b => b.BetsId[0]).ToArray() : null
#pragma warning restore IDE0037 // Use inferred member name
                    };
            }
        }

        public int[] RandomSequence()
        {
            var result = new int[Lottery.NumberSize];
            for (int i = 0; i < Lottery.NumberSize; i++)
            {
                int num;
                do
                {
                    num = Lottery.Rand.Next(0, Lottery.PoolSize);

                } while (result.Contains(num));

                result[i] = num;
            }
            return result.OrderBy(x=>x).ToArray();
        }

        public Task StartPlacingBets(Lottery lottery, int num)
        {
            var tasks = new List<Task>();
            for (int i = 0; i < num; i++)
            {
                var t = Task.Run(async () =>
                {
                    try
                    {
                       // await Task.Delay(Lottery.Rand.Next(500, 1000));
                        var name = UserOperations.GetName();
                        var user = await UserOperations.RegisterOrLoad(Lottery.CasinoTestInstance,$"{name}@karmel.com", name);
                        for (int j = 0; j < 10; j++)
                        {
                        //    await Task.Delay(Lottery.Rand.Next(500, 1000));
                            await user.PlaceBet(Lottery.CasinoTestInstance, lottery.Id, RandomSequence(), Lottery.Rand.Next(1, 10));
                        }
                    }
                    catch (ConcurrencyException)
                    {
                        // expected
                    }
                    catch (InsufficientFunds)
                    {
                        // expected
                    }
                    catch (JavaScriptException e) when(e.Message.Contains("Lottery is over!"))
                    {
                        // expected
                    }
                    catch (Exception)
                    {
                        // not expected at all!
                        // WriteLine(e);
                        throw;
                    }
                });
                tasks.Add(t);
            }

            return Task.WhenAll(tasks);
        }

        public void ChangeBets(Lottery lottery, int num)
        {
            for (int i = 0; i < num; i++)
            {
                Task.Run(async () =>
                {
                    await Task.Delay(Lottery.Rand.Next(100, 1000));
                    var name = UserOperations.GetName();
                    var user = await UserOperations.RegisterOrLoad(Lottery.CasinoTestInstance,$"{name}@karmel.com", name);
                    while (true)
                    {
                        await Task.Delay(Lottery.Rand.Next(100, 500));
                        await user.PlaceBet(Lottery.CasinoTestInstance, lottery.Id, RandomSequence(), Lottery.Rand.Next(1, 10));
                    }
                });
            }
        }

        public int ReplicaCount()
        {
            return DocumentStore.Maintenance.Server.Send(new GetDatabaseRecordOperation(DocumentStore.Database)).Topology.Count - 1;
        }

        public async Task CreateAndRunLottery()
        {
            var policy = Policy.Handle<TimeoutException>().Retry(5);
            // Write("Creating Lottery ... ");
            var lottery = await Lottery.CreateLottery(this);

            ReportInfo($"Lottery {lottery.Id} was created and will overdue at {lottery.DueTime}");
            // WriteLine($"Done {lottery.Id}");

            // WriteLine("Start betting");
            var t = StartPlacingBets(lottery, 1000);
            ReportInfo($"Start betting in lottery {lottery.Id}");

            var sleep = (int)(lottery.DueTime - DateTime.UtcNow).TotalMilliseconds;
            if (sleep > 10)
                await Task.Delay(sleep);

            ReportInfo($"Lottery {lottery.Id} is overdue");

            // Write("Finalize bets ... ");
            await policy.Execute(lottery.FinalizeBets);
            // WriteLine("Done");

            ReportInfo($"Rolling the dice for lottery {lottery.Id}");
            // WriteLine("Rolling the Dice ... ");
            lottery.RollTheDice();
            // Write("Completing the lottery ... ");

            await policy.Execute(lottery.Complete);
            ReportInfo($"Lottery {lottery.Id} is completed");
            // WriteLine("Done");

            var profit = await policy.Execute(lottery.GetFinalBettingReport);
            ReportInfo($"Report for lottery {lottery.Id} was generated and winners were rewarded.");
            // WriteLine(profit);

            await t;
        }

        public override void RunActualTest()
        {
            for (int i = 0; i < 3; i++)
            {
                var t = Task.Run(CreateAndRunLottery);

                try
                {
                    t.Wait();
                }
                catch (Exception e)
                {
                    ReportFailure("Lottery failed", e);
                }
            }
        }
    }
}
