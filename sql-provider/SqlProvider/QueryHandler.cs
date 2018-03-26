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

        public static void OnErrorExecute(Exception ex)
        {
            OnError?.Invoke(ex);
            Linq.Linq.OnErrorExecute(ex);
        }

        public class ConnStringElement
        {
            public string ConnString;
            public long Priority;
            public TimeSpan SleepOnFail = new TimeSpan(0, 0, 0);
            public DateTime NotUseFor = DateTime.Now;

            public void SetFail()
            {
                NotUseFor = DateTime.Now + SleepOnFail;
            }
        }
        public class ConnStringGroup
        {
            public List<ConnStringElement> Connects;
            public ConnStringElement GetConnString()
            {
                return Connects.Where(f => f.NotUseFor <= DateTime.Now).OrderBy(f => f.Priority).FirstOrDefault();
            }

            /// <summary>
            /// Создание нового подключения
            /// </summary>
            /// <param name="lo">List of Dictionary of string-object  values: (string)conn_string, (long)priority, (int)fail_timeout [in seconds]</param>
            /// <returns></returns>
            public static ConnStringGroup Create(List<object> lo)
            {
                var csg = new ConnStringGroup();
                csg.Connects = lo?.Select(f => new ConnStringElement()
                {
                    ConnString = f.GetElement_DO<string>("conn_string"),
                    Priority = f.GetElement_DO<long>("priority"),
                    SleepOnFail = new TimeSpan(0, 0, (int)f.GetElement_DO<long>("fail_timeout"))
                }).ToList() ?? new List<ConnStringElement>();

                return csg;
            }
        }

        public static Dictionary<string, string> ConnStrings = new Dictionary<string, string>();
        public static Dictionary<string, ConnStringGroup> ConnGroups = new Dictionary<string, ConnStringGroup>();

        public static bool Execute(this string query, string csName, string connectionString, out DataResult result
            , int timeout = 10, int error1205LevelHanle = 10)
        {
            if (csName == null)
            {
                return query.Execute(out result, connectionString, timeout, error1205LevelHanle);                
            }
            else
            {
                return query.Execute(csName, out result, timeout, error1205LevelHanle);
            }
        }
        public static bool Execute(this string query, string csName, out DataResult result
            , int timeout = 10, int error1205LevelHanle = 10)
        {
            TryGetConnString(csName, out var connectionString, out var element);

            var res = query.Execute(out result, connectionString, timeout, error1205LevelHanle);

            if (!res)
            {
                element?.SetFail();
            }

            return res;
        }

        public static bool Execute(this string query, string csName
            , int timeout = 10, int error1205LevelHanle = 10)
        {
            DataResult result;
            TryGetConnString(csName, out var connectionString, out var element);

            var res = query.Execute(out result, connectionString, timeout, error1205LevelHanle);

            if (!res)
            {
                element?.SetFail();
            }

            return res;
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
                            cmd.CommandTimeout = timeout;
                            using (var reader = cmd.ExecuteReader())
                            {
                                while (reader.FieldCount > 0)
                                {
                                    List<Tuple<Type, string>> ft = new List<Tuple<Type, string>>();
                                    Dictionary<string, Type> b_name = new Dictionary<string, Type>();

                                    for (int i = 0; i < reader.FieldCount; i++)
                                    {
                                        Type t = reader.GetFieldType(i);
                                        string field_name = reader.GetName(i);
                                        if (b_name.ContainsKey(field_name)) { field_name = field_name + i.ToString(); }
                                        ft.Add(new Tuple<Type, string>(t == typeof(DBNull) ? null : t, field_name));
                                        b_name.Add(field_name, t);
                                    }
                                    result.res_types.Add(ft);
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

        public static bool Execute_Step(this string query, string csName, out DataResult result, int batch_limit
            , Func<List<Tuple<Type, string>>, List<Dictionary<string, object>>, bool> func
            , int timeout = 3600, int error1205LevelHanle = 10)
        {

            TryGetConnString(csName, out var connectionString, out var element);

            var res = query.Execute_Step(out result, connectionString, batch_limit, func, timeout, error1205LevelHanle);

            if (!res)
            {
                element?.SetFail();
            }

            return res;
        }

        public static bool Execute_Step(this string query, string csName, string connectionString, out DataResult result, int batch_limit
            , Func<List<Tuple<Type, string>>, List<Dictionary<string, object>>, bool> func
            , int timeout = 3600, int error1205LevelHanle = 10)
        {
            if (csName == null)
            {
                return query.Execute_Step(out result, connectionString, batch_limit, func, timeout, error1205LevelHanle);
            }
            else
            {
                return query.Execute_Step(csName, out result, batch_limit, func, timeout, error1205LevelHanle);
            }
        }

        public static bool Execute_Step(this string query, string csName, int batch_limit
            , Func<List<Tuple<Type, string>>, List<Dictionary<string, object>>, bool> func
            , int timeout = 3600, int error1205LevelHanle = 10)
        {
            DataResult result;
            TryGetConnString(csName, out var connectionString, out var element);

            var res = query.Execute_Step(out result, connectionString, batch_limit, func, timeout, error1205LevelHanle);

            if (!res)
            {
                element?.SetFail();
            }

            return res;
        }
        public static bool Execute_Step(this string query, out DataResult result, string connectionString, int batch_limit
            , Func<List<Tuple<Type, string>>, List<Dictionary<string, object>>, bool> func
            , int timeout = 3600, int error1205LevelHanle = 10)
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
                            cmd.CommandTimeout = timeout;
                            using (var reader = cmd.ExecuteReader())
                            {
                                while (reader.FieldCount > 0)
                                {
                                    List<Tuple<Type, string>> ft = new List<Tuple<Type, string>>();
                                    Dictionary<string, Type> b_name = new Dictionary<string, Type>();
                                    
                                    for (int i = 0; i < reader.FieldCount; i++)
                                    {
                                        Type t = reader.GetFieldType(i);
                                        string field_name = reader.GetName(i);
                                        if (b_name.ContainsKey(field_name)) { field_name = field_name + i.ToString(); }
                                        ft.Add(new Tuple<Type, string>(t == typeof(DBNull) ? null : t, field_name));
                                        b_name.Add(field_name, t);
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

                                        if (lr.Count >= batch_limit)
                                        {
                                            if (!func(ft, lr))
                                            {
                                                Exception ex = new Exception("function faild in Execute_Step");
                                                result.e = ex;
                                                OnErrorExecute(ex);
                                                return false;
                                            }
                                            lr = new List<Dictionary<string, object>>();
                                        }
                                    }
                                    if (lr.Count > 0)
                                    {
                                        if (!func(ft, lr))
                                        {
                                            Exception ex = new Exception("function faild in Execute_Step");
                                            result.e = ex;
                                            OnErrorExecute(ex);
                                            return false;
                                        }
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
                    result.se = new Exception("fail_to_call");
                    OnErrorExecute(ex);
                }
            } while (error1205 && error1205LevelHanle >= 0);

            return res;
        }



        public static bool Execute_Bulk(this string table_name, string csName, string connectionString, out DataResult result, List<Tuple<Type, string>> columns, List<Dictionary<string, object>> data
            , int timeout = 3600, int error1205LevelHanle = 10)
        {
            if (csName == null)
            {
                return table_name.Execute_Bulk(out result, connectionString, columns, data, timeout, error1205LevelHanle);
            }
            else
            {
                return table_name.Execute_Bulk(csName, out result, columns, data, timeout, error1205LevelHanle);
            }            
        }
        public static bool Execute_Bulk(this string table_name, string csName, out DataResult result, List<Tuple<Type, string>> columns, List<Dictionary<string, object>> data
            , int timeout = 3600, int error1205LevelHanle = 10)
        {
            TryGetConnString(csName, out var connectionString, out var element);

            var res = table_name.Execute_Bulk(out result, connectionString, columns, data, timeout, error1205LevelHanle);

            if (!res)
            {
                element?.SetFail();
            }

            return res;
        }
        public static bool Execute_Bulk(this string table_name, string csName, List<Tuple<Type, string>> columns, List<Dictionary<string, object>> data
            , int timeout = 3600, int error1205LevelHanle = 10)
        {
            DataResult result;
            TryGetConnString(csName, out var connectionString, out var element);

            var res = table_name.Execute_Bulk(out result, connectionString, columns, data, timeout, error1205LevelHanle);

            if (!res)
            {
                element?.SetFail();
            }

            return res;
        }
        public static bool Execute_Bulk(this string table_name, out DataResult result, string connectionString, List<Tuple<Type, string>> columns, List<Dictionary<string, object>> data
            , int timeout = 3600, int error1205LevelHanle = 10)
        {
            bool res = false;
            result = new DataResult();

            DataTable dt = new DataTable(table_name);
            {
                for (int i = 0; i < columns.Count; i++)
                {
                    DataColumn dc = new DataColumn();
                    dc.ColumnName = columns[i].Item2;
                    dc.DataType = columns[i].Item1;
                    dt.Columns.Add(dc);
                }

                for (int j = 0; j < data.Count; j++)
                {
                    DataRow row = dt.NewRow();
                    for (int i = 0; i < columns.Count; i++)
                    {
                        row[columns[i].Item2] = data[j].GetElement(columns[i].Item2) ?? DBNull.Value;
                    }
                    dt.Rows.Add(row);
                }
            }


            bool error1205 = false;
            do
            {
                error1205 = false;
                try
                {
                    using (var conn = new SqlConnection(connectionString))
                    {
                        conn.Open();
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                        {
                            bulkCopy.DestinationTableName =
                                table_name;

                            bulkCopy.WriteToServer(dt);

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
                    result.se = new Exception("fail_to_call");
                    OnErrorExecute(ex);
                }
            } while (error1205 && error1205LevelHanle >= 0);

            return res;
        }


        public static void TryGetConnString(string csName, out string connectionString, out ConnStringElement element)
        {
            if (ConnStrings.TryGetValue(csName, out connectionString))
            {
                element = null;
                return;
            }
            if (ConnGroups.TryGetValue(csName, out var group))
            {
                element = group.GetConnString();
                connectionString = element?.ConnString;
                return;
            }

            connectionString = null;
            element = null;
        }
    }
}

