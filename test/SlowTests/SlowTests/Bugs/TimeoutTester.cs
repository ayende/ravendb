using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace SlowTests.SlowTests.Bugs
{
    public class TimeoutTester : RavenTestBase
    {
        private class AnswerVote
        {
            public string QuestionId { get; set; }
            public string AnswerId { get; set; }
            public int Delta { get; set; }
            public decimal DecimalValue { get; set; }
        }

        private class User
        {
            public string Id { get; set; }
            public string DisplayName { get; set; }
        }

        private class AnswerVoteEntity
        {
            public string Id { get; set; }
            public string QuestionId { get; set; }
            public AnswerEntity Answer { get; set; }
            public int Delta { get; set; }
        }

        private class AnswerEntity
        {
            public string Id { get; set; }
            public string UserId { get; set; }
            public Question Question { get; set; }
            public string Content { get; set; }
        }

        private class Answer
        {
            public string Id { get; set; }
            public string UserId { get; set; }
            public string QuestionId { get; set; }
            public string Content { get; set; }
        }

        private class Question
        {
            public string Id { get; set; }
            public string UserId { get; set; }
            public string Title { get; set; }
            public string Content { get; set; }
        }

        private class Answers_ByAnswerEntity : AbstractIndexCreationTask<Answer>
        {
            public Answers_ByAnswerEntity()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  AnswerId = doc.Id,
                                  UserId = doc.UserId,
                                  QuestionId = doc.QuestionId,
                                  Content = doc.Content
                              };

                Index(x => x.Content, FieldIndexing.Analyzed);
                Index(x => x.UserId, FieldIndexing.NotAnalyzed); // Case-sensitive searches
            }
        }

        private class Answers_ByAnswerEntityTransformer : AbstractTransformerCreationTask<Answer>
        {
            public Answers_ByAnswerEntityTransformer()
            {
                TransformResults = results =>
                    from result in results
                    let question = LoadDocument<Question>(result.QuestionId)
                    select new // AnswerEntity
                    {
                        Id = result.Id,
                        Question = question,
                        Content = result.Content,
                        UserId = result.UserId
                    };
            }
        }

        [Fact]
        public async Task will_timeout_query_after_some_time()
        {
            using (var store = await GetDocumentStore())
            {
                new Answers_ByAnswerEntity().Execute(store);
                new Answers_ByAnswerEntityTransformer().Execute(store);
                var answerId = "";

                store.Conventions.MaxNumberOfRequestsPerSession = 1000000; // 1 Million
                CreateEntities(store, 0);

                const string content = "This is doable";

                using (var session = store.OpenSession())
                {
                    RavenQueryStatistics stats;
                    AnswerEntity answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
                        .Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
                        .OrderBy(x => x.Content)
                        .Where(x => x.Content == (content))
                        .TransformWith<Answers_ByAnswerEntityTransformer, AnswerEntity>()
                        .Skip(0).Take(1)
                        .FirstOrDefault();

                    Assert.NotNull(answerInfo);
                    answerId = answerInfo.Id;
                }
                List<Task> tasks = new List<Task>();
                object locker = new object();
                for (int k = 0; k < 100; k++)
                {
                    var thread = Task.Factory.StartNew(() =>
                    {
                        lock (locker)
                        {
                            using (var session = store.OpenSession())
                            {
                                for (int i = 0; i < 100; i++)
                                {
                                    var answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
                                        .OrderBy(x => x.Content)
                                        .TransformWith<Answers_ByAnswerEntityTransformer, AnswerEntity>()
                                        .Skip(0).Take(1)
                                        .FirstOrDefault();

                                    Assert.NotNull(answerInfo);
                                }
                            }
                            using (var session = store.OpenSession())
                            {
                                var answer = session.Load<Answer>(answerId);
                                Assert.NotNull(answer);

                                answer.Content += k.ToString();
                                session.Store(answer);
                                session.SaveChanges();
                            }
                        }
                    }, TaskCreationOptions.LongRunning);
                    tasks.Add(thread);
                }

                Task.WaitAll(tasks.ToArray());
            }
        }

        public static string CreateEntities(IDocumentStore documentStore, int index)
        {
            string questionId = @"question/259" + index;
            string answerId = @"answer/540" + index;
            using (IDocumentSession session = documentStore.OpenSession())
            {
                var user = new User { Id = @"user/222" + index, DisplayName = "John Doe" + index };
                session.Store(user);

                var question = new Question
                {
                    Id = questionId,
                    Title = "How to do this in RavenDb?" + index,
                    Content = "I'm trying to find how to model documents for better DDD support." + index,
                    UserId = @"user/222" + index
                };
                session.Store(question);

                var answer = new AnswerEntity
                {
                    Id = answerId,
                    Question = question,
                    Content = "This is doable",
                    UserId = user.Id
                };

                session.Store(new Answer
                {
                    Id = answer.Id,
                    UserId = answer.UserId,
                    QuestionId = answer.Question.Id,
                    Content = answer.Content
                });

                var vote1 = new AnswerVoteEntity { Id = "votes/1" + index, Answer = answer, QuestionId = questionId, Delta = 2 };
                session.Store(new AnswerVote
                {
                    QuestionId = vote1.QuestionId,
                    AnswerId = vote1.Answer.Id,
                    Delta = vote1.Delta
                });

                var vote2 = new AnswerVoteEntity { Id = "votes/2" + index, Answer = answer, QuestionId = questionId, Delta = 3 };
                session.Store(new AnswerVote
                {
                    QuestionId = vote2.QuestionId,
                    AnswerId = vote2.Answer.Id,
                    Delta = vote2.Delta
                });

                session.SaveChanges();
            }
            return answerId;
        }


    }
}
