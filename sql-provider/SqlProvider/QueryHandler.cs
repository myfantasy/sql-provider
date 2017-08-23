using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace System.Data
{

    public static class QueryHandler
    {
        public static event Action<Exception> OnError;

        private static void OnErrorExecute(Exception ex)
        {
            if (OnError != null)
            {
                OnError(ex);
            }
        }

        public static Dictionary<string, string> ConnStrings = new Dictionary<string, string>();

        public static bool Execute(this string query, string csName, out DataResult result
            , int timeout = 10, int error1205LevelHanle = 10)
        {
            return query.Execute(out result, ConnStrings[csName], timeout, error1205LevelHanle);
        }

        public static bool Execute(this string query, string csName
            , int timeout = 10, int error1205LevelHanle = 10)
        {
            DataResult result;
            return query.Execute(out result, ConnStrings[csName], timeout, error1205LevelHanle);
        }

        public static bool Execute(this string query, out DataResult result, string connectionString
            , int timeout = 10, int error1205LevelHanle = 10)
        {
            bool res = false;
            result = new DataResult();
            bool error1205 = false;
            do
            {
                error1205 = false;
                try
                {
                    using (var conn = new SqlConnection(connectionString))
                    {
                        conn.Open();
                        using (var cmd = new SqlCommand())
                        {
                            cmd.Connection = conn;
                            cmd.CommandText = query;
                            using (var reader = cmd.ExecuteReader())
                            {
                                while (reader.FieldCount > 0)
                                {
                                    List<Tuple<Type, string>> ft = new List<Tuple<Type, string>>();
                                    Dictionary<string, bool> b_name = new Dictionary<string, bool>();

                                    for (int i = 0; i < reader.FieldCount; i++)
                                    {
                                        Type t = reader.GetFieldType(i);
                                        string field_name = reader.GetName(i);
                                        if (b_name.ContainsKey(field_name)) { field_name = field_name + i.ToString(); }
                                        ft.Add(new Tuple<Type, string>(t == typeof(DBNull) ? null : t, field_name));
                                        b_name.Add(field_name, true);
                                    }
                                    List<Dictionary<string, object>> lr = new List<Dictionary<string, object>>();
                                    while (reader.Read())
                                    {
                                        Dictionary<string, object> row = new Dictionary<string, object>();
                                        for (int i = 0; i < ft.Count; i++)
                                        {
                                            object v = ft[i].Item1 == null ? null : reader.GetValue(i);
                                            if (v is DBNull) { v = null; }
                                            row.Add(ft[i].Item2, v);
                                        }
                                        lr.Add(row);
                                    }
                                    result.res.Add(lr);
                                    reader.NextResult();
                                }
                            }
                        }
                    }
                    return true;
                }
                catch (SqlException se)
                {
                    if (se.Number == 1205)
                    {
                        error1205 = true;
                        error1205LevelHanle--;
                    }
                    else
                    {
                        result.e = se;
                        result.se = se;
                        OnErrorExecute(se);
                    }
                }
                catch (Exception ex)
                {
                    result.e = ex;
                    OnErrorExecute(ex);
                }
            } while (error1205 && error1205LevelHanle >= 0);

            return res;
        }
    }
}

