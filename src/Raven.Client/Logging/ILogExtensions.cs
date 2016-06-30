using System.Diagnostics;

namespace Raven.Abstractions.Logging
{
    using System;
    using System.Globalization;

    public static class ILogExtensions
    {
	    public static bool TurnOnPrinfDebugging = false;

        public static void Debug(this ILog logger, string message, params object[] args)
        {
            GuardAgainstNullLogger(logger);
			#if DEBUG
				if(TurnOnPrinfDebugging) Console.WriteLine("[debug]" + String.Format(message,args));
			#endif
            logger.Log(LogLevel.Debug, () =>
            {
                if (args == null || args.Length == 0)
                    return message;
                return string.Format(CultureInfo.InvariantCulture, message, args);
            });
        }

        public static void DebugException(this ILog logger, string message, Exception ex)
        {
            GuardAgainstNullLogger(logger);
			#if DEBUG
				if(TurnOnPrinfDebugging) Console.WriteLine("[debug exception]" + message + "->" + ex);
			#endif
            logger.Log(LogLevel.Debug, () => message, ex);
        }

        public static void Error(this ILog logger, string message, params object[] args)
        {
            GuardAgainstNullLogger(logger);
			#if DEBUG
				if(TurnOnPrinfDebugging) Console.WriteLine("[error]" + String.Format(message,args));
			#endif
            logger.Log(LogLevel.Error, () =>
            {
                if (args == null || args.Length == 0)
                    return message;
                return string.Format(CultureInfo.InvariantCulture, message, args);
            });
        }

        public static void ErrorException(this ILog logger, string message, Exception exception)
        {
            GuardAgainstNullLogger(logger);
			#if DEBUG
				if(TurnOnPrinfDebugging) Console.WriteLine("[exception]" + message + "->" + exception);
			#endif

            logger.Log(LogLevel.Error, () => message, exception);
        }

        public static void FatalException(this ILog logger, string message, Exception exception)
        {
            GuardAgainstNullLogger(logger);
			#if DEBUG
				if(TurnOnPrinfDebugging) Console.WriteLine("[fatal exception]" + message + "->" + exception);
			#endif

            logger.Log(LogLevel.Fatal, () => message, exception);
        }

        public static void Info(this ILog logger, string message, params object[] args)
        {
            GuardAgainstNullLogger(logger);
			#if DEBUG
				if(TurnOnPrinfDebugging) Console.WriteLine("[info]" + String.Format(message,args));
			#endif
            logger.Log(LogLevel.Info, () =>
            {
                if (args == null || args.Length == 0)
                    return message;
                return string.Format(CultureInfo.InvariantCulture, message, args);
            });
        }

        public static void InfoException(this ILog logger, string message, Exception exception)
        {
            GuardAgainstNullLogger(logger);
            logger.Log(LogLevel.Info, () => message, exception);
        }

        public static void Warn(this ILog logger, string message, params object[] args)
        {
            GuardAgainstNullLogger(logger);
			#if DEBUG
				if(TurnOnPrinfDebugging) Console.WriteLine("[warn]" + String.Format(message,args));
			#endif
            logger.Log(LogLevel.Warn, () =>
            {
                if (args == null || args.Length == 0)
                    return message;
                return string.Format(CultureInfo.InvariantCulture, message, args);
            });
        }

        public static void WarnException(this ILog logger, string message, Exception ex)
        {
            GuardAgainstNullLogger(logger);
            logger.Log(LogLevel.Warn, () => message, ex);
        }

        private static void GuardAgainstNullLogger(ILog logger)
        {
            if(logger == null)
            {
                throw new ArgumentException("logger is null", "logger");
            }
        }
    }
}
