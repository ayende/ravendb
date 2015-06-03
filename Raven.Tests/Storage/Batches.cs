using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Database.Storage;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Storage
{
	public class Batches : RavenTest
	{
		[Theory]
		[PropertyData("Storages")]
		public void BatchNestingAndCommits(string storageType)
		{
			int commitsCalled = 0;
			using (var ts = NewTransactionalStorage(storageType, onCommit: () => commitsCalled++))
			{
				ts.Batch(x => { });
				Assert.Equal(1, commitsCalled);
				commitsCalled = 0;
				ts.Batch(x => { ts.Batch(y => { }); });
				Assert.Equal(1, commitsCalled);

				commitsCalled = 0;
				ts.Batch(x =>
				{
					using (ts.DisableBatchNesting())
					{
						ts.Batch(y => { });
					}
				});
				Assert.Equal(2, commitsCalled);

				commitsCalled = 0;
				ts.Batch(x =>
				{
					using (ts.DisableBatchNesting(true))
					{
						ts.Batch(y => { });
					}
				});
				Assert.Equal(1, commitsCalled);

				commitsCalled = 0;
				using (ts.DisableBatchNesting())
				{
					ts.Batch(x => { ts.Batch(y => { }); });
				}
				Assert.Equal(2, commitsCalled);

				commitsCalled = 0;
				using (ts.DisableBatchNesting(true))
				{
					ts.Batch(x => { ts.Batch(y => { }); });
				}
				Assert.Equal(1, commitsCalled);

				commitsCalled = 0;
				ts.Batch(x => { ts.Batch(y => { ts.Batch(z => { }); }); });
				Assert.Equal(1, commitsCalled);

				commitsCalled = 0;
				ts.Batch(x =>
				{
					using (ts.DisableBatchNesting())
					{
						ts.Batch(y => { ts.Batch(z => { }); });
					}
				});
				Assert.Equal(3, commitsCalled);

				commitsCalled = 0;
				ts.Batch(x =>
				{
					using (ts.DisableBatchNesting(true))
					{
						ts.Batch(y => { ts.Batch(z => { }); });
					}
				});
				Assert.Equal(1, commitsCalled);


				commitsCalled = 0;
				using (ts.DisableBatchNesting())
				{
					ts.Batch(x => { ts.Batch(y => { ts.Batch(z => { }); }); });
				}
				Assert.Equal(3, commitsCalled);

				commitsCalled = 0;
				using (ts.DisableBatchNesting(true))
				{
					ts.Batch(x => { ts.Batch(y => { ts.Batch(z => { }); }); });
				}
				Assert.Equal(1, commitsCalled);

				commitsCalled = 0;
				ts.Batch(x =>
				{
					ts.Batch(y =>
					{
						using (ts.DisableBatchNesting())
						{
							ts.Batch(z => { });
						}
					});
				});
				Assert.Equal(2, commitsCalled);

				commitsCalled = 0;
				ts.Batch(x =>
				{
					ts.Batch(y =>
					{
						using (ts.DisableBatchNesting(true))
						{
							ts.Batch(z => { });
						}
					});
				});
				Assert.Equal(1, commitsCalled);

				commitsCalled = 0;
				ts.Batch(x =>
				{
					ts.Batch(y =>
					{
						for (var i = 0; i < 10; i++)
						{
							ts.Batch(z => { });
						}
					});
				});
				Assert.Equal(1, commitsCalled);

				commitsCalled = 0;
				using (ts.DisableBatchNesting())
					ts.Batch(x =>
					{
						ts.Batch(y =>
						{
							for (var i = 0; i < 10; i++)
							{
								ts.Batch(z => { });
							}
						});
					});
				Assert.Equal(12, commitsCalled);

				commitsCalled = 0;
				using (ts.DisableBatchNesting(true))
					ts.Batch(x =>
					{
						ts.Batch(y =>
						{
							for (var i = 0; i < 10; i++)
							{
								ts.Batch(z => { });
							}
						});
					});
				Assert.Equal(1, commitsCalled);

				commitsCalled = 0;
				ts.Batch(x =>
				{
					using (ts.DisableBatchNesting())
						ts.Batch(y =>
						{
							for (var i = 0; i < 10; i++)
							{
								ts.Batch(z => { });
							}
						});
				});
				Assert.Equal(12, commitsCalled);

				commitsCalled = 0;
				ts.Batch(x =>
				{
					using (ts.DisableBatchNesting(true))
						ts.Batch(y =>
						{
							for (var i = 0; i < 10; i++)
							{
								ts.Batch(z => { });
							}
						});
				});
				Assert.Equal(1, commitsCalled);

				commitsCalled = 0;
				ts.Batch(x =>
				{
					ts.Batch(y =>
					{
						for (var i = 0; i < 10; i++)
						{
							using (ts.DisableBatchNesting())
								ts.Batch(z => { });
						}
					});
				});
				Assert.Equal(11, commitsCalled);

				commitsCalled = 0;
				ts.Batch(x =>
				{
					ts.Batch(y =>
					{
						for (var i = 0; i < 10; i++)
						{
							using (ts.DisableBatchNesting(true))
								ts.Batch(z => { });
						}
					});
				});
				Assert.Equal(1, commitsCalled);
			}
		}
	}
}