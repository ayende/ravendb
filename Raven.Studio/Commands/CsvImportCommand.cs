﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Controls;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Util;
using Raven.Json.Linq;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Kent.Boogaart.KBCsv;
using TaskStatus = Raven.Studio.Models.TaskStatus;

namespace Raven.Studio.Commands
{
	public class CsvImportCommand : Command
	{
		const int BatchSize = 512;
		private readonly Action<string> output;
		private readonly CsvImportTaskSectionModel taskModel;

		public CsvImportCommand(CsvImportTaskSectionModel taskModel, Action<string> output)
		{
			this.output = output;
			this.taskModel = taskModel;
		}

		public override void Execute(object parameter)
		{
			var openFile = new OpenFileDialog
			{
				Filter = "csv|*.csv"
			};

			if (openFile.ShowDialog() != true)
				return;

			taskModel.TaskStatus = TaskStatus.Started;
			output(string.Format("Importing from {0}", openFile.File.Name));

			var streamReader = openFile.File.OpenText();

			var importImpl = new ImportImpl(streamReader, openFile.File.Name, taskModel, output, DatabaseCommands);
			importImpl.ImportAsync()
				.ContinueWith(task =>
				{
					importImpl.Dispose();
					return task;
				})
				.Unwrap()
				.Catch();
		}

		public class ImportImpl : IDisposable
		{
			private readonly CsvImportTaskSectionModel taskModel;
			private readonly Action<string> output;
			private readonly IAsyncDatabaseCommands databaseCommands;
			private readonly CsvReader csvReader;
			private readonly HeaderRecord header;
			private readonly string entity;
			private readonly Stopwatch sw;
			private IEnumerator<DataRecord> enumerator;
			private int totalCount;
			private bool hadError = false;

			public ImportImpl(StreamReader reader, string file, CsvImportTaskSectionModel taskModel, Action<string> output, IAsyncDatabaseCommands databaseCommands)
			{
				this.taskModel = taskModel;
				this.output = output;
				this.databaseCommands = databaseCommands;
				csvReader = new CsvReader(reader);
				header = csvReader.ReadHeaderRecord();
				entity = Inflector.Pluralize(CSharpClassName.ConvertToValidClassName(Path.GetFileNameWithoutExtension(file)));
				if (entity.Length > 0 &&  char.IsLower(entity[0]))
					entity = char.ToUpper(entity[0]) + entity.Substring(1);

				sw = Stopwatch.StartNew();

				enumerator = csvReader.DataRecords.GetEnumerator();
			}

			public Task ImportAsync()
			{
				if (hadError)
					return new CompletedTask();

				var batch = new List<RavenJObject>();
				var columns = header.Values.Where(x => x.StartsWith("@") == false).ToArray();
				while (enumerator.MoveNext())
				{
					var record = enumerator.Current;
					var document = new RavenJObject();
					string id = null;
					RavenJObject metadata = null;
					foreach (var column in columns)
					{
						if (string.IsNullOrEmpty(column))
							continue;

						try
						{
							if (string.Equals("id", column, StringComparison.OrdinalIgnoreCase))
							{
								id = record[column];
							}
							else if (string.Equals(Constants.RavenEntityName, column, StringComparison.OrdinalIgnoreCase))
							{
								metadata = metadata ?? new RavenJObject();
								metadata[Constants.RavenEntityName] = record[column];
								id = id ?? record[column] + "/";
							}
							else if (string.Equals(Constants.RavenClrType, column, StringComparison.OrdinalIgnoreCase))
							{
								metadata = metadata ?? new RavenJObject();
								metadata[Constants.RavenClrType] = record[column];
								id = id ?? record[column] + "/";
							}
							else
							{
								document[column] = SetValueInDocument(record[column]);
							}
						}
						catch (Exception e)
						{
							Infrastructure.Execute.OnTheUI(() =>
							{
								taskModel.ReportError(e);
								taskModel.ReportError("Import not completed");
								taskModel.TaskStatus = TaskStatus.Ended;
								return new CompletedTask();
							});

						}
					}

					metadata = metadata ?? new RavenJObject { { "Raven-Entity-Name", entity } };
					document.Add("@metadata", metadata);
					metadata.Add("@id", id ?? Guid.NewGuid().ToString());

					batch.Add(document);

					if (batch.Count >= BatchSize)
					{
						return FlushBatch(batch)
							.ContinueWith(t =>
							{
								if (t.IsFaulted)
								{
									Infrastructure.Execute.OnTheUI(() =>
									{
										taskModel.ReportError(t.Exception);
										taskModel.ReportError("Import not completed");
										taskModel.TaskStatus = TaskStatus.Ended;
									});

									return t;
								}
								return t.IsCompleted ? ImportAsync() : t;
							})
							.Unwrap();
					}
				}

				if (batch.Count > 0)
				{
					return FlushBatch(batch)
						.ContinueWith(t =>
						{
							if (t.IsFaulted)
							{
								Infrastructure.Execute.OnTheUI(() =>
								{
									taskModel.ReportError(t.Exception);
									taskModel.ReportError("Import not completed");
									taskModel.TaskStatus = TaskStatus.Ended;
								});

								return t;
							}

							return t.IsCompleted ? ImportAsync() : t;
						})
						.Unwrap();
				}

				output(String.Format("Imported {0:#,#;;0} documents in {1:#,#;;0} ms", totalCount, sw.ElapsedMilliseconds));
				taskModel.TaskStatus = TaskStatus.Ended;

				return new CompletedTask();
			}

			private static RavenJToken SetValueInDocument(string value)
			{
				if (string.IsNullOrEmpty(value))
					return value;

				var ch = value[0];
				if (ch == '[' || ch == '{')
				{
					try
					{
						return RavenJToken.Parse(value);
					}
					catch (Exception)
					{
						// ignoring failure to parse, will proceed to insert as a string value
					}
				}
				else if (char.IsDigit(ch) || ch == '-' || ch == '.')
				{
					// maybe it is a number?
					long longResult;
					if (long.TryParse(value, out longResult))
					{
						return longResult;
					}

					decimal decimalResult;
					if (decimal.TryParse(value, out decimalResult))
					{
						return decimalResult;
					}
				}
				else if (ch == '"' && value.Length > 1 && value[value.Length - 1] == '"')
				{
					return value.Substring(1, value.Length - 2);
				}

				return value;
			}

			Task FlushBatch(ICollection<RavenJObject> batch)
			{
				totalCount += batch.Count;
				var sw = Stopwatch.StartNew();
				var commands = (from doc in batch
								let metadata = doc.Value<RavenJObject>("@metadata")
								let removal = doc.Remove("@metadata")
								select new PutCommandData
								{
									Metadata = metadata,
									Document = doc,
									Key = metadata.Value<string>("@id"),
								}).ToArray();

				output(String.Format("Wrote {0} documents  in {1:#,#;;0} ms",
									 batch.Count, sw.ElapsedMilliseconds));

				return databaseCommands
					.BatchAsync(commands);
			}

			/// <summary>
			/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
			/// </summary>
			public void Dispose()
			{
				csvReader.Close();
			}
		}
	}
}