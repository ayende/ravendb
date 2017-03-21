using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                using (var testclass = new FastTests.Client.FirstClassPatch())
                {
                    testclass.CanPatchAndModify();
                }
                Console.WriteLine(i);
            }
        }
    }
}