using Raven.Database.Plugins;

namespace Raven.Bundles.Quotas.Documents.Triggers
{
	public class DocumentCountQuotaForDocumentDeleteTrigger : AbstractDeleteTrigger
	{
		public override void AfterDelete(string key, Abstractions.Data.TransactionInformation transactionInformation)
		{
			DocQuotaConfiguration.GetConfiguration(Database).AfterDelete();
		}
	}
}