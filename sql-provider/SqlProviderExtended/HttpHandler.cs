using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace System.Net.Http
{

    public static class HttpQuery
    {

        public static Dictionary<string, string> McServers = new Dictionary<string, string>();
        public static Dictionary<string, QueryHandler.ConnStringGroup> McGroups = new Dictionary<string, QueryHandler.ConnStringGroup>();

        private static void AddHeaders(this HttpHeaders headersCollection, Dictionary<string, string> addingHeaders)
        {
            if (headersCollection == null || addingHeaders == null)
            {
                return;
            }

            foreach (var item in addingHeaders)
            {
                if (headersCollection.Any(x => string.Equals(x.Key, item.Key, StringComparison.OrdinalIgnoreCase)))
                {
                    continue;
                }

                headersCollection.Add(item.Key, item.Value);
            }
        }

        public static async Task<Tuple<string, System.Net.HttpStatusCode>> CallService(this string path, string serviceId, string serviceUrl, Dictionary<string, object> args = null, bool sendJson = true, int timeoutSeconds = 10,
            string httpMethod = "POST", Dictionary<string, string> headers = null)
        {
            if (serviceId == null)
            {
                return await CallService(serviceUrl, args, sendJson, timeoutSeconds, httpMethod, headers);
            }
            else
            {
                return await CallService(path, serviceId, args, sendJson, timeoutSeconds, httpMethod, headers);
            }
        }

        public static async Task<Tuple<string, System.Net.HttpStatusCode>> CallService(this string path, string serviceId, Dictionary<string, object> args = null, bool sendJson = true, int timeoutSeconds = 10,
            string httpMethod = "POST", Dictionary<string, string> headers = null)
        {
            TryGetMcServers(serviceId, out var serviceUrl, out var element);
            
            if (serviceUrl == null)
            {
                return new Tuple<string, HttpStatusCode>($"Не найден service_id = {serviceId}", HttpStatusCode.BadRequest);
            }

            var res = await CallService(serviceUrl + path, args, sendJson, timeoutSeconds, httpMethod, headers);

            if ((int)res.Item2 >= 400 && (int)res.Item2 < 409 || (int)res.Item2 >= 500)
            {
                element.SetFail();
            }

            return res;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="serviceUrl"></param>
        /// <param name="args">Важно, что Если аргумент содержит ключ "" (Пустая строка) то отправится значение только из него</param>
        /// <param name="sendJson"></param>
        /// <param name="timeoutSeconds"></param>
        /// <param name="httpMethod"></param>
        /// <param name="headers"></param>
        /// <returns></returns>
        public static async Task<Tuple<string, HttpStatusCode>> CallService(string serviceUrl, Dictionary<string, object> args = null, bool sendJson = true, int timeoutSeconds = 10,
            string httpMethod = "POST", Dictionary<string, string> headers = null)
        {
            StringContent content;
            if (sendJson)
            {
                if (args.ContainsKey(""))
                {
                    content = new StringContent(args[""].TryGetJson());
                }
                else
                {
                    content = new StringContent(args.TryGetJson());
                }
            }
            else
            {
                var argsText = args?.Count > 0
                    ? string.Join("&", args.Select(x => $"{x.Key}={x.Value?.ToString()}"))
                    : "";

                content = new StringContent(argsText);
            }
            string result = null;
            content.Headers.Remove("content-type");
            if (sendJson)
            {
                content.Headers.Add("content-type", "application/json");
            }
            else
            {
                content.Headers.Add("content-type", "application/x-www-form-urlencoded");
            }

            var hsc = HttpStatusCode.NotFound;
            var client = new HttpClient();
            client.Timeout = TimeSpan.FromSeconds(timeoutSeconds);
            client.DefaultRequestHeaders.AddHeaders(headers);

            try
            {
                var request = new HttpRequestMessage(new HttpMethod(httpMethod), serviceUrl) { Content = content };
                var q1 = await client.SendAsync(request);

                result = await q1.Content.ReadAsStringAsync();
                hsc = q1.StatusCode;
                return new Tuple<string, HttpStatusCode>(result, hsc);
            }
            catch (Exception e)
            {
                return new Tuple<string, HttpStatusCode>(e.Message, HttpStatusCode.ServiceUnavailable);
            }            
        }


        public static async Task<Tuple<string, System.Net.HttpStatusCode>> CallServiceGet(this string path, string serviceId, string serviceUrl, Dictionary<string, object> args = null, int timeoutSeconds = 10,
            Dictionary<string, string> headers = null)
        {
            if (serviceId == null)
            {
                return await CallServiceGet(serviceUrl, args, timeoutSeconds, headers);
            }
            else
            {
                return await CallServiceGet(path, serviceId, args, timeoutSeconds, headers);
            }
        }

        public static async Task<Tuple<string, System.Net.HttpStatusCode>> CallServiceGet(this string path, string serviceId, Dictionary<string, object> args = null, int timeoutSeconds = 10,
            Dictionary<string, string> headers = null)
        {
            TryGetMcServers(serviceId, out var serviceUrl, out var element);

            if (serviceUrl == null)
            {
                return new Tuple<string, HttpStatusCode>($"Не найден service_id = {serviceId}", HttpStatusCode.BadRequest);
            }

            var res = await CallServiceGet(serviceUrl + path, args, timeoutSeconds, headers);

            if ((int)res.Item2 >= 400 && (int)res.Item2 < 409 || (int)res.Item2 >= 500)
            {
                element.SetFail();
            }

            return res;
        }

        public static async Task<Tuple<string, HttpStatusCode>> CallServiceGet(string serviceUrl, Dictionary<string, object> args = null, int timeoutSeconds = 10,
            Dictionary<string, string> headers = null)
        {
            StringBuilder sb = new StringBuilder("");

            if (args != null && args.Count >= 1)
            {
                bool is_first = true;
                foreach (var p in args)
                {
                    sb.Append((is_first ? "?" : "&") + p.Key + "=" + WebUtility.UrlEncode((p.Value ?? "").ToString()));
                    is_first = false;
                }
            }

            serviceUrl = serviceUrl + sb.ToString();

            string result = null;
            HttpStatusCode hsc = HttpStatusCode.NotFound;
            HttpClient client = new HttpClient();
            client.Timeout = new TimeSpan(0, 0, timeoutSeconds);
            client.DefaultRequestHeaders.AddHeaders(headers);

            try
            {
                var q1 = await client.GetAsync(serviceUrl);

                result = await q1.Content.ReadAsStringAsync();
                hsc = q1.StatusCode;
                return new Tuple<string, HttpStatusCode>(result, hsc);
            }
            catch (Exception e)
            {
                return new Tuple<string, HttpStatusCode>(e.Message, HttpStatusCode.ServiceUnavailable);
            }
        }


        public static void TryGetMcServers(string serviceId, out string serviceUrl, out QueryHandler.ConnStringElement element)
        {
            if (McServers.TryGetValue(serviceId, out serviceUrl))
            {
                element = null;
                return;
            }
            if (McGroups.TryGetValue(serviceId, out var group))
            {
                element = group.GetConnString();
                serviceUrl = element?.ConnString;
                return;
            }

            serviceUrl = null;
            element = null;
        }
    }
}

