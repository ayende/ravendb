using System.Globalization;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class HugeDocumentsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/huge-documents", "GET")]
        public Task HugeDocuments()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                writer.WriteStartArray();

                var isFirst = true;

                foreach (var key in context.DocumentDatabase().HugeDocuments.GetHugeDocuments())
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(context.GetLazyString("Id"));
                    writer.WriteString(context.GetLazyString(key.Item1));

                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString("Size"));
                    writer.WriteInteger(context.DocumentDatabase().HugeDocuments.GetSize(key));

                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString("Last Access"));
                    writer.WriteString(context.GetLazyString(key.Item2.ToString(CultureInfo.InvariantCulture)));

                    writer.WriteEndObject();

                }

                writer.WriteEndArray();
            }

            return Task.CompletedTask;
        }
    }
}