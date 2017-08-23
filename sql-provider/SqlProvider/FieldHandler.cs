using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace System.Data
{
    /// <summary>
    /// Помошник работы с полями
    /// </summary>
    public static class FieldHandler
    {
        /// <summary>
        /// Конвертирует в строку готовую ко вставке в запрос в качестве параметра
        /// </summary>
        /// <param name="value">Входящее значение</param>
        /// <returns>строка сконвентированная</returns>
        public static string ConvertValueToDB(object value)
        {
            if (value == null)
            {
                return "null";
            }
            else if (value is DateTime)
            {
                return "'" + ((DateTime)value).ToString("yyyyMMdd HH:mm:ss.fffffff") + "'";
            }
            else if (value is TimeSpan)
            {
                return "'" + ((TimeSpan)value).ToString(@"hh\:mm\:ss\.fffffff") + "'";
            }
            else if (value is string)
            {
                return "'" + ((string)value).Replace("'", "''") + "'";
            }
            else if (value is byte[])
            {
                StringBuilder sb = new StringBuilder("0x");
                foreach (byte b in (byte[])value)
                {
                    sb.Append(b.ToString("X2"));
                }
                sb.Append("");
                return sb.ToString();
            }
            else if (value is int)
            {
                return ((int)value).ToString();
            }
            else if (value is long)
            {
                return ((long)value).ToString();
            }
            else if (value is long?)
            {
                if (((long?)value).HasValue)
                {
                    return ((long?)value).Value.ToString();
                }
                else
                {
                    return null;
                }
            }
            else if (value is byte)
            {
                return ((byte)value).ToString();
            }
            else if (value is short)
            {
                return ((short)value).ToString();
            }
            else if (value is short?)
            {
                if (((short?)value).HasValue)
                {
                    return ((short?)value).Value.ToString();
                }
                else
                {
                    return null;
                }
            }
            else if (value is bool)
            {
                if (((bool)value))
                {
                    return "1";
                }
                else
                {
                    return "0";
                }
            }
            else if (value is double)
            {
                return ((double)value).ToString().Replace(",", ".");
            }
            else if (value is decimal)
            {
                return ((decimal)value).ToString().Replace(",", ".");
            }
            else
            {
                return "'" + value.ToString() + "'";
            }
        }

        public static string ConvertToDB<T>(this T value)
        {
            return ConvertValueToDB(value);
        }

        public static string ConvertToDB<T>(this IEnumerable<T> list, string tableName, Func<T, string> cfti = null)
        {
            if (cfti == null)
            {
                cfti = ConvertToDB;
            }
            return TIdsListAdd(list, tableName, cfti);
        }

        public static string convert_to_param_list(this Dictionary<string, object> DO, string table_name = "@params", string prefix = "")
        {
            StringBuilder sb = new StringBuilder("");
            if (DO != null)
                foreach (var kvp in DO)
                {
                    if (kvp.Value is Dictionary<string, object>)
                    {
                        sb.AppendLine((kvp.Value as Dictionary<string, object>).convert_to_param_list(table_name, kvp.Key + "."));
                    }
                    else if (kvp.Value is List<object>)
                    {
                        sb.AppendLine(
                             "insert into " + table_name + " select " + (prefix + kvp.Key).ConvertToDB() + ", " + (kvp.Value as List<object>).TryGetJson(kvp.Key).ConvertToDB() + ";"
                            );
                    }
                    else
                    {
                        sb.AppendLine(
                            "insert into " + table_name + " select " + (prefix + kvp.Key).ConvertToDB() + ", " + kvp.Value.ConvertToDB() + ";"
                            );
                    }
                }

            return sb.ToString();
        }

        /// <summary>
        /// Добавление данных в запрос из списка Id
        /// </summary>
        /// <param name="ids">список Id</param>
        /// <param name="tableName">Имя таблицы</param>
        /// <returns>строку подмены запроса</returns>
        public static string TIdsListAdd(IEnumerable<long> ids, string tableName)
        {
            if ((ids != null) && (ids.Count() > 0))
            {
                string tableInsertBegin = "insert into " + tableName + " values ";

                StringBuilder res = new StringBuilder("");
                bool isFirst = true;
                int row = 0;
                int rowMax = 500;
                foreach (long id in ids)
                {
                    if (isFirst)
                    {
                        isFirst = !isFirst;
                        res.AppendLine(tableInsertBegin);
                    }
                    else
                    {
                        res.Append(",");
                    }
                    res.Append("(");
                    res.Append(FieldHandler.ConvertValueToDB(id));
                    res.Append(")");
                    row++;

                    if (row >= rowMax)
                    {
                        row = 0;
                        isFirst = true;
                    }
                }
                return res.ToString();
            }
            else
            {
                return "";
            }
        }

        /// <summary>
        /// Добавление данных в запрос из списка Id
        /// </summary>
        /// <param name="ids">список Id</param>
        /// <param name="tableName">Имя таблицы</param>
        /// <returns>строку подмены запроса</returns>
        public static string TIdsListAdd<T>(IEnumerable<T> ids, string tableName, Func<T, string> cfti)
        {
            if ((ids != null) && (ids.Count() > 0))
            {
                string tableInsertBegin = "insert into " + tableName + " values ";

                StringBuilder res = new StringBuilder("");
                bool isFirst = true;
                int row = 0;
                int rowMax = 500;
                foreach (T id in ids)
                {
                    if (isFirst)
                    {
                        isFirst = !isFirst;
                        res.AppendLine(tableInsertBegin);
                    }
                    else
                    {
                        res.Append(",");
                    }
                    res.Append("(");
                    res.Append(cfti(id));
                    res.Append(")");
                    row++;

                    if (row >= rowMax)
                    {
                        row = 0;
                        isFirst = true;
                    }
                }
                return res.ToString();
            }
            else
            {
                return "";
            }
        }

    }
}
