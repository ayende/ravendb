using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Actions;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
    public class DocumentsBatchController : ClusterAwareRavenDbApiController
    {
        [HttpPost]
        [RavenRoute("bulk_docs")]
        [RavenRoute("databases/{databaseName}/bulk_docs")]
        public async Task<HttpResponseMessage> BulkPost()
        {
            using (var cts = new CancellationTokenSource())
            using (cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
            {
                RavenJArray jsonCommandArray;

                try
                {
                    jsonCommandArray = await ReadJsonArrayAsync().ConfigureAwait(false);
                }
                catch (InvalidOperationException e)
                {
                    if (Log.IsDebugEnabled)
                        Log.DebugException("Failed to deserialize document batch request." , e);
                    return GetMessageWithObject(new
                    {
                        Message = "Could not understand json, please check its validity."
                    }, (HttpStatusCode)422); //http code 422 - Unprocessable entity
                    
                }
                catch (InvalidDataException e)
                {
                    if (Log.IsDebugEnabled)
                        Log.DebugException("Failed to deserialize document batch request." , e);
                    return GetMessageWithObject(new
                    {
                        e.Message
                    }, (HttpStatusCode)422); //http code 422 - Unprocessable entity
                }

                cts.Token.ThrowIfCancellationRequested();

                var transactionInformation = GetRequestTransaction();
                var commands =
                    (from RavenJObject jsonCommand in jsonCommandArray select CommandDataFactory.CreateCommand(jsonCommand, transactionInformation)).ToArray();

                if (Log.IsDebugEnabled)
                    Log.Debug(
                    () =>
                    {
                        if (commands.Length > 15) // this is probably an import method, we will input minimal information, to avoid filling up the log
                        {
                            return "\tExecuted "
                                   + string.Join(
                                       ", ", commands.GroupBy(x => x.Method).Select(x => string.Format("{0:#,#;;0} {1} operations", x.Count(), x.Key)));
                        }

                        var sb = new StringBuilder();
                        foreach (var commandData in commands)
                        {
                            sb.AppendFormat("\t{0} {1}{2}", commandData.Method, commandData.Key, Environment.NewLine);
                        }
                        return sb.ToString();
                    });

                var batchResult = Database.Batch(commands, cts.Token);
                return GetMessageWithObject(batchResult);
            }
        }

        [HttpDelete]
        [RavenRoute("bulk_docs/{*id}")]
        [RavenRoute("databases/{databaseName}/bulk_docs/{*id}")]
        public HttpResponseMessage BulkDelete(string id)
        {
            var indexDefinition = Database.IndexDefinitionStorage.GetIndexDefinition(id);
            if (indexDefinition == null)
                throw new IndexDoesNotExistsException(string.Format("Index '{0}' does not exist.", id));

            if (indexDefinition.IsMapReduce)
                throw new InvalidOperationException("Cannot execute DeleteByIndex operation on Map-Reduce indexes.");

            // we don't use using because execution is async
            var cts = new CancellationTokenSource();
            var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout);

            var databaseBulkOperations = new DatabaseBulkOperations(Database, GetRequestTransaction(), cts, timeout);
            return OnBulkOperation((index, query, options, reportProgress) => databaseBulkOperations.DeleteByIndex(index, query, options, reportProgress), id, timeout);
        }

        [HttpPatch]
        [RavenRoute("bulk_docs/{*id}")]
        [RavenRoute("databases/{databaseName}/bulk_docs/{*id}")]
        public async Task<HttpResponseMessage> BulkPatch(string id)
        {
            RavenJArray patchRequestJson;
            try
            {
                patchRequestJson = await ReadJsonArrayAsync().ConfigureAwait(false);
            }
            catch (InvalidOperationException e)
            {
                if (Log.IsDebugEnabled)
                    Log.DebugException("Failed to deserialize document batch request." , e);
                return GetMessageWithObject(new
                {
                    Message = "Could not understand json, please check its validity."
                }, (HttpStatusCode)422); //http code 422 - Unprocessable entity

            }
            catch (InvalidDataException e)
            {
                if (Log.IsDebugEnabled)
                    Log.DebugException("Failed to deserialize document batch request." , e);
                return GetMessageWithObject(new
                {
                    e.Message
                }, (HttpStatusCode)422); //http code 422 - Unprocessable entity
            }

            // we don't use using because execution is async
            var cts = new CancellationTokenSource();
            var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout);

            var databaseBulkOperations = new DatabaseBulkOperations(Database, GetRequestTransaction(), cts, timeout);

            var patchRequests = patchRequestJson.Cast<RavenJObject>().Select(PatchRequest.FromJson).ToArray();
            return OnBulkOperation((index, query, options, reportProgress) => databaseBulkOperations.UpdateByIndex(index, query, patchRequests, options, reportProgress), id, timeout);
        }

        [HttpEval]
        [RavenRoute("bulk_docs/{*id}")]
        [RavenRoute("databases/{databaseName}/bulk_docs/{*id}")]
        public async Task<HttpResponseMessage> BulkEval(string id)
        {
            RavenJObject advPatchRequestJson;

            try
            {
                advPatchRequestJson = await ReadJsonObjectAsync<RavenJObject>().ConfigureAwait(false);
            }
            catch (InvalidOperationException e)
            {
                if (Log.IsDebugEnabled)
                    Log.DebugException("Failed to deserialize document batch request." , e);
                return GetMessageWithObject(new
                {
                    Message = "Could not understand json, please check its validity."
                }, (HttpStatusCode)422); //http code 422 - Unprocessable entity

            }
            catch (InvalidDataException e)
            {
                if (Log.IsDebugEnabled)
                    Log.DebugException("Failed to deserialize document batch request." , e);
                return GetMessageWithObject(new
                {
                    e.Message
                }, (HttpStatusCode)422); //http code 422 - Unprocessable entity
            }

            // we don't use using because execution is async
            var cts = new CancellationTokenSource();
            var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout);

            var databaseBulkOperations = new DatabaseBulkOperations(Database, GetRequestTransaction(), cts, timeout);

            var advPatch = ScriptedPatchRequest.FromJson(advPatchRequestJson);
            return OnBulkOperation((index, query, options, reportProgress) => databaseBulkOperations.UpdateByIndex(index, query, advPatch, options, reportProgress), id, timeout);
        }

        private HttpResponseMessage OnBulkOperation(Func<string, IndexQuery, BulkOperationOptions, Action<BulkOperationProgress>, RavenJArray> batchOperation, string index, CancellationTimeout timeout)
        {
            if (string.IsNullOrEmpty(index))
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            var option = new BulkOperationOptions
            {
                AllowStale = GetAllowStale(),
                MaxOpsPerSec = GetMaxOpsPerSec(),
                StaleTimeout = GetStaleTimeout(),
                RetrieveDetails = GetRetrieveDetails()
            };

            var indexQuery = GetIndexQuery(maxPageSize: int.MaxValue);

            var status = new BulkOperationStatus();
            long id;

            var task = Task.Factory.StartNew(() =>
            {
                status.State["Batch"] = batchOperation(index, indexQuery, option, x =>
                {
                    status.MarkProgress(x);
                });
            }).ContinueWith(t =>
            {
                if (timeout != null)
                    timeout.Dispose();

                if (t.IsFaulted == false)
                {
                    status.MarkCompleted($"Processed {status.OperationProgress.ProcessedEntries} items");
                    return;
                }

                var exception = t.Exception.ExtractSingleInnerException();

                status.MarkFaulted(exception.Message);
            });

            Database.Tasks.AddTask(task, status, new TaskActions.PendingTaskDescription
                                                 {
                                                     StartTime = SystemTime.UtcNow,
                                                     TaskType = TaskActions.PendingTaskType.IndexBulkOperation,
                                                     Description = index
                                                 }, out id, timeout.CancellationTokenSource);

            return GetMessageWithObject(new { OperationId = id }, HttpStatusCode.Accepted);
        }

        public class BulkOperationStatus : OperationStateBase
        {
            public RavenJArray Result { get; set; }
            public BulkOperationProgress OperationProgress { get; private set; }

            public BulkOperationStatus()
            {
                OperationProgress = new BulkOperationProgress();
            }

            public void MarkProgress(BulkOperationProgress progress)
            {
                OperationProgress.ProcessedEntries = progress.ProcessedEntries;
                OperationProgress.TotalEntries = progress.TotalEntries;
                State["Progress"] = $"Processed {progress.ProcessedEntries}/{progress.TotalEntries} items";
            }
        }
    }
}
