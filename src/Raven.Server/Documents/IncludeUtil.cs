using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Json;
using Voron.Util;

namespace Raven.Server.Documents
{
    public static class IncludeUtil
    {	   
        public static IEnumerable<string> GetDocIdFromInclude(BlittableJsonReaderObject docReader, string includePath)
        {
            var indexOfFirstSeparator = includePath.IndexOfAny(new []{'.',','}, 0);
            if (indexOfFirstSeparator == -1) //we have simple case - flat property
            {
                string val;
                if (docReader.TryGet(includePath, out val))
                    yield return val;			    
            }
            else //start analyzing include path
            {
                var currentReader = docReader;
                
                var currentIndex = indexOfFirstSeparator;
                var propertyNameStringBuilder = new StringBuilder();

                while (currentIndex < includePath.Length)
                {
                    if (includePath[currentIndex] == ' ')
                    {
                        currentIndex++;
                        continue;
                    }

                    if (includePath[currentIndex] == '.')
                    {
                        if(includePath[currentIndex + 1] == '.' ||
                           includePath[currentIndex + 1] == ',')
                            throw new InvalidOperationException("Invalid include path - it should not contain adjacent dot (.) characters");

                        var propertyName = propertyNameStringBuilder.Length == 0 ? 
                            includePath.Substring(0,currentIndex) : 
                            propertyNameStringBuilder.ToString();
                        propertyNameStringBuilder.Clear();
                        object property;
                        if (currentReader.TryGet(propertyName, out property))
                        {
                            var reader = property as BlittableJsonReaderObject;
                            if (reader != null)
                            {
                                currentReader = reader;
                                currentIndex++;
                                continue;							    
                            }

                            var array = property as BlittableJsonReaderArray;
                            if (array != null)
                            {
                                //TODO : do not forget to handle this case
                                //throw exception here?
                                currentIndex++;
                                continue;
                            }

                            var str = property as string;
                            if (str != null && currentIndex < includePath.Length - 1)
                            {
                                //nowhere to continue, since we have reached the maximum
                                //depth in the object
                                yield break; 
                            }
                        }
                    }

                    if (includePath[currentIndex] == ',')
                    {
                        if (includePath[currentIndex + 1] == ',' ||
                            includePath[currentIndex + 1] == '.')
                            throw new InvalidOperationException("Invalid include path - it should not contain adjacent dot (.) characters");

                        var propertyName = propertyNameStringBuilder.ToString();
                        propertyNameStringBuilder.Clear();
                        string id;
                        if (currentReader.TryGet(propertyName, out id))
                            yield return id;

                        propertyNameStringBuilder.Clear();
                        currentReader = docReader;
                        currentIndex++;
                        continue;
                    }

                    if (currentIndex == includePath.Length - 1) //we are at the end of the path
                    {
                        propertyNameStringBuilder.Append(includePath[currentIndex]);
                        var propertyName = propertyNameStringBuilder.ToString();
                        propertyNameStringBuilder.Clear();
                        string id;
                        if (currentReader.TryGet(propertyName, out id))
                        {
                            yield return id;
                            yield break;
                        }
                    }				   

                    propertyNameStringBuilder.Append(includePath[currentIndex]);
                    currentIndex++;
                }
                yield break;
            }		  
        }
    }
}
