using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace System.Data
{

    public static class QueryHandlerPg
    {
        public static int BUFFER_SIZE_BYTES = 1024 * 10;
        public static int MAX_READ_TRY_COUNT = 2;

        public static Dictionary<string, string> ConnStrings = new Dictionary<string, string>();
        public static Dictionary<string, QueryHandler.ConnStringGroup> ConnGroups = new Dictionary<string, QueryHandler.ConnStringGroup>();

        public static bool ExecutePg(this string query, string csName, string connectionString, out DataResult result
            , int timeout = 10)
        {
            if (csName == null)
            {
                return query.ExecutePg(out result, connectionString, timeout);
            }
            else
            {
                return query.ExecutePg(csName, out result, timeout);
            }
        }

        public static bool ExecutePg(this string query, string csName, out DataResult result
            , int timeout = 10)
        {
            TryGetConnString(csName, out var connectionString, out var element);

            var res = query.ExecutePg(out result, connectionString, timeout);

            if (!res)
            {
                element?.SetFail();
            }

            return res;
        }

        public static bool ExecutePg(this string query, string csName
            , int timeout = 10)
        {
            DataResult result;
            TryGetConnString(csName, out var connectionString, out var element);

            var res = query.ExecutePg(out result, connectionString, timeout);

            if (!res)
            {
                element?.SetFail();
            }

            return res;
        }

        public static bool ExecutePg(this string query, out DataResult result, string connectionString, int timeout = 30)
        {

            result = new DataResult();
            try
            {
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    conn.Open();

                    using (var cmd = new Npgsql.NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.CommandText = "SET statement_timeout = " + timeout.ToString() + "000;";
                        cmd.ExecuteNonQuery();
                    }

                    using (var cmd = new Npgsql.NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.CommandText = query;
                        cmd.CommandTimeout = timeout;
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
            catch (PostgresException se)
            {
                result.e = se;
                result.se = se;
                QueryHandler.OnErrorExecute(se);
            }
            catch (Exception ex)
            {
                result.e = ex;
                QueryHandler.OnErrorExecute(ex);
            }
            return false;
        }

        public static bool ExecutePg_Step(this string query, string csName, string connectionString, out DataResult result, int batch_limit
            , Func<List<Tuple<Type, string>>, List<Dictionary<string, object>>, bool> func
            , int timeout = 3600)
        {
            if (csName == null)
            {
                return query.ExecutePg_Step(out result, connectionString, batch_limit, func, timeout);
            }
            else
            {
                return query.ExecutePg_Step(csName, out result, batch_limit, func, timeout);
            }
        }
        public static bool ExecutePg_Step(this string query, string csName, out DataResult result, int batch_limit
            , Func<List<Tuple<Type, string>>, List<Dictionary<string, object>>, bool> func
            , int timeout = 3600)
        {
            TryGetConnString(csName, out var connectionString, out var element);

            var res = query.ExecutePg_Step(out result, connectionString, batch_limit, func, timeout);

            if (!res)
            {
                element?.SetFail();
            }

            return res;
        }

        public static bool ExecutePg_Step(this string query, string csName, int batch_limit
            , Func<List<Tuple<Type, string>>, List<Dictionary<string, object>>, bool> func
            , int timeout = 3600)
        {
            DataResult result;
            TryGetConnString(csName, out var connectionString, out var element);

            var res = query.ExecutePg_Step(out result, connectionString, batch_limit, func, timeout);

            if (!res)
            {
                element?.SetFail();
            }

            return res;
        }
        public static bool ExecutePg_Step(this string query, out DataResult result, string connectionString, int batch_limit
            , Func<List<Tuple<Type, string>>, List<Dictionary<string, object>>, bool> func, int timeout = 3600)
        {

            result = new DataResult();
            try
            {
                using (var conn = new Npgsql.NpgsqlConnection(connectionString))
                {
                    conn.Open();

                    using (var cmd = new Npgsql.NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.CommandText = "SET statement_timeout = " + timeout.ToString() + "000;";
                        cmd.ExecuteNonQuery();
                    }

                    using (var cmd = new Npgsql.NpgsqlCommand())
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

                                    if (lr.Count > batch_limit)
                                    {
                                        if (!func(ft, lr))
                                        {
                                            Exception ex = new Exception("function faild in ExecutePg_Step");
                                            result.e = ex;
                                            QueryHandler.OnErrorExecute(ex);
                                            return false;
                                        }
                                        lr = new List<Dictionary<string, object>>();
                                    }
                                }
                                if (lr.Count > 0)
                                {
                                    if (!func(ft, lr))
                                    {
                                        Exception ex = new Exception("function faild in ExecutePg_Step");
                                        result.e = ex;
                                        QueryHandler.OnErrorExecute(ex);
                                        return false;
                                    }
                                }
                                reader.NextResult();
                            }
                        }
                    }
                }
                return true;
            }
            catch (NpgsqlException se)
            {
                result.e = se;
                result.se = se;
                QueryHandler.OnErrorExecute(se);
            }
            catch (Exception ex)
            {
                result.e = ex;
                result.se = new Exception("fail_to_call");
                QueryHandler.OnErrorExecute(ex);
            }
            return false;
        }








        public static Exception ReadBinary(string query, string conn_string, out byte[] result, int timeoutMilliseconds = 100)
        {
            try
            {
                using (var sourceConnection = new NpgsqlConnection(conn_string))
                {
                    sourceConnection.Open();

                    using (var memoryStream = new MemoryStream())
                    {
                        using (var inStream = sourceConnection.BeginRawBinaryCopy($"COPY ({query}) TO STDOUT (FORMAT BINARY)"))
                        {
                            var buffer = new byte[BUFFER_SIZE_BYTES];
                            var tryNumber = 0;

                            while (tryNumber <= MAX_READ_TRY_COUNT)
                            {
                                tryNumber++;
                                int readLength;
                                while ((readLength = inStream.Read(buffer, 0, buffer.Length)) > 0)
                                {
                                    memoryStream.Write(buffer, 0, readLength);
                                    tryNumber = 0;
                                }

                                Thread.Sleep(timeoutMilliseconds);
                            }
                        }

                        sourceConnection.Close();
                        result = memoryStream.ToArray();
                        return null;
                    }
                }
            }
            catch (Exception ex)
            {
                result = null;
                QueryHandler.OnErrorExecute(ex);
                return ex;
            }
        }

        private static string GenerateFormatText(this object value)
        {
            if (value == null)
            {
                return "\\N";
            }

            if (value is bool boolValue)
            {
                return boolValue ? "t" : "f";
            }

            if (value is DateTime dateTime)
            {
                return dateTime.ToString("yyyyMMdd HH:mm:ss.fffffff");
            }

            if (value is TimeSpan timeSpan)
            {
                return timeSpan.ToString(@"hh\:mm\:ss\.fffffff");
            }

            if (value is float floatValue)
            {
                return floatValue.ToString().Replace(",", ".");
            }

            if (value is double doubleValue)
            {
                return doubleValue.ToString().Replace(",", ".");
            }

            if (value is decimal decimalValue)
            {
                return decimalValue.ToString().Replace(",", ".");
            }

            if (value is byte[] bytes)
            {
                return $"\\\\x{string.Concat(bytes.Select(x => x.ToString("X2")))}";
            }

            var stringValue = value.ToString();
            return stringValue.Replace("\\", "\\\\")
                .Replace("\r", "\\r").Replace("\n", "\\n")
                .Replace("\t", "\\t").Replace("\v", "\\v")
                .Replace("\b", "\\b").Replace("\f", "\\f");
        }

        public static Exception CopyCN(string sourceQuery, string destinationTableName, string sourceConnectionName, string destinationConnectionName, int timeoutMilliseconds = 100)
        {
            TryGetConnString(sourceConnectionName, out var sourceConnectionString, out var elementSource);
            TryGetConnString(destinationConnectionName, out var destinationConnectionString, out var elementDestination);

            var res = Copy(sourceQuery, destinationTableName, sourceConnectionName, destinationConnectionName, timeoutMilliseconds);
            if (res != null)
            {
                elementSource?.SetFail();
                elementDestination?.SetFail();
            }

            return res;
        }
        public static Exception Copy(string sourceQuery, string destinationTableName, string sourceConnectionString, string destinationConnectionString, int timeoutMilliseconds = 100)
        {
            try
            {
                using (var sourceConnection = new NpgsqlConnection(sourceConnectionString))
                using (var destinationConnection = new NpgsqlConnection(destinationConnectionString))
                {
                    sourceConnection.Open();
                    destinationConnection.Open();

                    using (var inStream = sourceConnection.BeginRawBinaryCopy($"COPY ({sourceQuery}) TO STDOUT (FORMAT BINARY)"))
                    using (var outStream = destinationConnection.BeginRawBinaryCopy($"COPY {destinationTableName} FROM STDIN (FORMAT BINARY)"))
                    {
                        var buffer = new byte[BUFFER_SIZE_BYTES];
                        var tryNumber = 0;

                        while (tryNumber <= MAX_READ_TRY_COUNT)
                        {
                            tryNumber++;
                            int readLength;
                            while ((readLength = inStream.Read(buffer, 0, buffer.Length)) > 0)
                            {
                                outStream.Write(buffer, 0, readLength);
                                tryNumber = 0;
                            }

                            Thread.Sleep(timeoutMilliseconds);
                        }
                    }

                    sourceConnection.Close();
                    destinationConnection.Close();
                }

                return null;
            }
            catch (Exception exception)
            {
                QueryHandler.OnErrorExecute(exception);
                return exception;
            }
        }

        public static Exception Copy(List<Dictionary<string, object>> datas, List<Tuple<Type,string>> fieldNames, string destinationTableName, string destinationConnectionName, string destinationConnectionString)
        {
            if (destinationConnectionName == null)
            {
                return Copy(destinationConnectionString, datas, fieldNames, destinationTableName);
            }
            else
            {
                return Copy(datas, fieldNames, destinationTableName, destinationConnectionName);
            }
        }
        public static Exception Copy(List<Dictionary<string, object>> datas, List<Tuple<Type, string>> fieldNames, string destinationTableName, string destinationConnectionName)
        {
            TryGetConnString(destinationConnectionName, out var destinationConnectionString, out var element);

            var res = Copy(destinationConnectionString, datas, fieldNames, destinationTableName);

            if (res != null)
            {
                element?.SetFail();
            }

            return res;
        }
        public static Exception Copy(string destinationConnectionString, List<Dictionary<string, object>> datas, List<Tuple<Type, string>> fieldNames, string destinationTableName)
        {
            return Copy(destinationConnectionString, datas, fieldNames.Select(f=>f.Item2).ToList(), destinationTableName);
        }
        public static Exception Copy(IList<Dictionary<string, object>> datas, IList<string> fieldNames, string destinationTableName, string destinationConnectionName, string destinationConnectionString)
        {
            if (destinationConnectionName == null)
            {
                return Copy(destinationConnectionString, datas, fieldNames, destinationTableName);
            }
            else
            {
                return Copy(datas, fieldNames, destinationTableName, destinationConnectionName);
            }
        }
        public static Exception Copy(IList<Dictionary<string, object>> datas, IList<string> fieldNames, string destinationTableName, string destinationConnectionName)
        {
            TryGetConnString(destinationConnectionName, out var destinationConnectionString, out var element);

            var res = Copy(destinationConnectionString, datas, fieldNames, destinationTableName);

            if (res != null)
            {
                element?.SetFail();
            }

            return res;
        }
        public static Exception Copy(string destinationConnectionString, IList<Dictionary<string, object>> datas, IList<string> fieldNames, string destinationTableName)
        {
            try
            {
                if (datas == null || !datas.Any())
                {
                    return null;
                }

                if (fieldNames == null || !fieldNames.Any())
                {
                    return null;
                }

                using (var destinationConnection = new NpgsqlConnection(destinationConnectionString))
                {
                    destinationConnection.Open();

                    var destinationQuery = $"{destinationTableName}({string.Join(",", fieldNames)})";
                    using (var writer = destinationConnection.BeginTextImport($"COPY {destinationQuery} FROM STDIN"))
                    {
                        foreach (var item in datas)
                        {
                            var rowString = string.Join("\t", fieldNames.Select(name => $"{item[name].GenerateFormatText()}"));
                            writer.Write($"{rowString}\n");
                        }

                        writer.Write("\\.\n");
                    }

                    destinationConnection.Close();
                    return null;
                }
            }
            catch (Exception exception)
            {
                QueryHandler.OnErrorExecute(exception);
                return exception;
            }
        }

        public static void TryGetConnString(string csName, out string connectionString, out QueryHandler.ConnStringElement element)
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

