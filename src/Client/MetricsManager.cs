using LibratoSharp.Client.Measurement;
using LibratoSharp.Client.Metric;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace LibratoSharp.Client
{
    public class MetricsManager
    {
        private const string METRICS_API_SERVER = "metrics-api.librato.com";
        private const string METRICS_URL = "https://metrics-api.librato.com/v1/metrics";
        private const string METRICS_PUT_POST = "/{0}";
        private const string REQUEST_CONTENT_TYPE = "application/json";
        private const string JSON_METRIC_NAME = "name";
        private const string JSON_METRIC_PERIOD = "period";
        private const string JSON_METRIC_DESCRIPTION = "description";
        private const string JSON_METRIC_DISPLAY_NAME = "display_name";
        private const string JSON_METRIC_ATRIBUTES = "attributes";
        private const string JSON_MEASUREMENT_VALUE = "value";
        private const string JSON_MEASUREMENT_TIME = "measurement_time";
        private const string JSON_MEASUREMENT_SOURCE = "source";

        private string _user;
        private string _apiToken;
        private HttpClient _client;

        public MetricsManager(string user, string apiToken)
        {
            if (string.IsNullOrEmpty(user) || string.IsNullOrEmpty(apiToken))
            {
                throw new ArgumentException("User and API Token are required");
            }
            this._user = user;
            this._apiToken = apiToken;
            this._client = new HttpClient();
        }

        public async Task PostMeasurement(IMeasurement measurement)
        {
            if (measurement == null)
            {
                return;
            }
            await PostMeasurement(new IMeasurement[]
			{
				measurement
			});
        }

        public async Task PostMeasurement(params IMeasurement[] measurements)
        {
            if (measurements == null || measurements.Length == 0)
            {
                return;
            }
            await PostMeasurement(measurements.ToList());
        }

        public async Task PostMeasurement(List<IMeasurement> measurements)
        {
            if (measurements == null || measurements.Count == 0)
            {
                return;
            }
            string json = this.CreateJsonObject(measurements);
            await MakeJsonPost(json, null, null);
        }

        private string CreateJsonObject(IEnumerable<IMeasurement> measurements)
        {
            StringBuilder sb = new StringBuilder();
            StringWriter sw = new StringWriter(sb);
            JsonWriter writer = new JsonTextWriter(sw);
            using (writer)
            {
                writer.Formatting = Formatting.Indented;
                writer.WriteStartObject();
                foreach (IGrouping<string, IMeasurement> measurementByType in measurements.GroupBy(measurement => measurement.Type))
                {
                    writer.WritePropertyName(measurementByType.Key + "s");
                    writer.WriteStartArray();
                    foreach (IMeasurement measurement in measurements.Where(m => m.Type == measurementByType.Key))
                    {
                        this.JsonWriteMeasurement(writer, measurement);
                    }
                    writer.WriteEnd();
                }
                writer.WriteEndObject();
            }
            return sb.ToString();
        }

        private string CreateJsonObject(IMetric metric)
        {
            StringBuilder sb = new StringBuilder();
            StringWriter sw = new StringWriter(sb);
            JsonWriter writer = new JsonTextWriter(sw);
            using (writer)
            {
                writer.Formatting = Formatting.Indented;
                writer.WriteStartObject();
                writer.WritePropertyName("type");
                writer.WriteValue(metric.Type);
                this.JsonAddMetricInfo(writer, metric, false);
                writer.WriteEndObject();
            }
            return sb.ToString();
        }

        private void JsonAddMetricInfo(JsonWriter writer, IMetric metric, bool includeName)
        {
            if (includeName)
            {
                writer.WritePropertyName("name");
                writer.WriteValue(metric.Name);
            }
            if (metric.DisplayName != null)
            {
                writer.WritePropertyName("display_name");
                writer.WriteValue(metric.DisplayName);
            }
            if (metric.Description != null)
            {
                writer.WritePropertyName("description");
                writer.WriteValue(metric.Description);
            }
            if (metric.Period != -1)
            {
                writer.WritePropertyName("period");
                writer.WriteValue(metric.Period);
            }
        }

        private void JsonWriteMeasurement(JsonWriter writer, IMeasurement measurement)
        {
            writer.WriteStartObject();
            this.JsonAddMetricInfo(writer, measurement, true);
            if (measurement.MeasurementTime != DateTime.MinValue && measurement.MeasurementTime != default(DateTime))
            {
                long unixTimestamp = this.GetUnixTimestamp(measurement.MeasurementTime);
                writer.WritePropertyName("measurement_time");
                writer.WriteValue(unixTimestamp.ToString());
            }
            if (measurement.Source != null)
            {
                writer.WritePropertyName("source");
                writer.WriteValue(measurement.Source);
            }
            if (measurement.Type == "gauge")
            {
                IGaugeMeasurement gaugeMeasurement = (IGaugeMeasurement)measurement;

                writer.WritePropertyName("count");
                writer.WriteValue(gaugeMeasurement.Count);

                writer.WritePropertyName("sum");
                writer.WriteValue(gaugeMeasurement.Sum);

                if (gaugeMeasurement.Max != default(object))
                {
                    writer.WritePropertyName("max");
                    writer.WriteValue(gaugeMeasurement.Max);
                }

                if (gaugeMeasurement.Min != default(object))
                {
                    writer.WritePropertyName("min");
                    writer.WriteValue(gaugeMeasurement.Min);
                }

                if (gaugeMeasurement.SumSquares != default(object))
                {
                    writer.WritePropertyName("sum_squares");
                    writer.WriteValue(gaugeMeasurement.SumSquares);
                }
            }
            else
            {
                writer.WritePropertyName("value");
                writer.WriteValue(measurement.Value.ToString());
            }
            writer.WriteEndObject();
        }

        public async Task CreateMetric(IMetric metric)
        {
            if (metric == null)
            {
                throw new ArgumentException("metric is null");
            }
            string json = this.CreateJsonObject(metric);
            string url = string.Format("/{0}", metric.Name);
            await MakeJsonPost(json, url, HttpMethod.Put);
        }

        public async Task DeleteMetric(IMetric metric)
        {
            if (metric == null)
            {
                throw new ArgumentException("metric is null");
            }
            string url = string.Format("/{0}", metric.Name);
            await MakeJsonPost(string.Empty, url, HttpMethod.Delete);
        }

        private async Task MakeJsonPost(string json, string urlPostfix, HttpMethod httpMethod)
        {
            string url = (urlPostfix == null) ? "https://metrics-api.librato.com/v1/metrics" : ("https://metrics-api.librato.com/v1/metrics" + urlPostfix);
            var request = new HttpRequestMessage(httpMethod ?? HttpMethod.Post, url);            
            SetBasicAuthHeader(request, _user, _apiToken);
            request.Headers.Add("User-Agent", ".NET API Client");

            request.Content = new StringContent(json, Encoding.UTF8, "application/json");
            var response = await _client.SendAsync(request);
            response.EnsureSuccessStatusCode();
        }

        private void SetBasicAuthHeader(HttpRequestMessage request, string userName, string userPassword)
        {
            string authInfo = userName + ":" + userPassword;
            authInfo = Convert.ToBase64String(Encoding.Default.GetBytes(authInfo));
            request.Headers.Add("Authorization", "Basic " + authInfo);
        }

        private long GetUnixTimestamp(DateTime dt)
        {
            DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0);
            return (long)Math.Floor((dt - origin).TotalSeconds);
        }
    }
}
