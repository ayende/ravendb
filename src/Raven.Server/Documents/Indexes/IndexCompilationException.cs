using System;
using Raven.Server.Extensions;

namespace Raven.Server.Documents.Indexes
{
    public class IndexCompilationException : Exception
    {
     
        public IndexCompilationException()
        {
        }

        public IndexCompilationException(string message)
            : base(message)
        {
        }

        public IndexCompilationException(string message, Exception inner)
            : base(message, inner)
        {
        }

        /// <summary>
        /// Indicates which property caused error (Maps, Reduce).
        /// </summary>
        public string IndexDefinitionProperty { get; set; }

        /// <summary>
        /// Value of a problematic property.
        /// </summary>
        public string ProblematicText { get; set; }

        public override string ToString()
        {
            return this.ExceptionToString(description =>
                                          description.AppendFormat(
                                              ", IndexDefinitionProperty='{0}', ProblematicText='{1}'",
                                              IndexDefinitionProperty,
                                              ProblematicText));
        }
    }
}
