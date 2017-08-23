using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace System.Data
{
    public class DataResult
    {
        public List<List<Dictionary<string, object>>> res = new List<List<Dictionary<string, object>>>();
        public Exception se;
        public Exception e;

        public bool exists_row { get { return ExistsRow(); } }

        public bool ExistsRow(int table = 0)
        {
            return res.Count > 0 && res[0].Count > 0;
        }
        public T GetElement<T>(string field_name)
        {
            return res[0][0].GetElement<T>(field_name);
        }

        public List<T> ToList<T>(Func<Dictionary<string, object>, T> parse, int table = 0)
        {
            return res[0].Select(f => parse(f)).ToList();
        }
    }

}
