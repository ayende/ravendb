using System;
using System.Diagnostics;

namespace Raven.Server
{
	//helper class to 'inject' exceptions for unit testing  purposes
    public static class DebugHelper
    {
	    static DebugHelper()
	    {
		    ThrowExceptionForDocumentReplicationReceive = () => false;
	    }

	    public static Func<bool> ThrowExceptionForDocumentReplicationReceive;

		[Conditional("UNIT_TEST_EXCEPTIONS")]
	    internal static void ThrowExceptionForDocumentReplicationReceiveIfRelevant()
	    {
		    if (ThrowExceptionForDocumentReplicationReceive())
			    throw new DebugHelperException("Debug Exception at document replication receive loop");
	    }

	    public class DebugHelperException : Exception
	    {
		    public DebugHelperException()
		    {
		    }

		    public DebugHelperException(string message) : base(message)
		    {
		    }

		    public DebugHelperException(string message, Exception innerException) : base(message, innerException)
		    {
		    }
	    }
	}
}
